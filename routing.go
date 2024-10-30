// Copyright 2018-2020 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package olric

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var routingUpdateMtx sync.Mutex
var routingSignature uint64

type route struct {
	Owners  []discovery.Member
	Backups []discovery.Member
}

type routingTable map[uint64]route

func (db *Olric) getReplicaOwners(partID uint64) ([]consistent.Member, error) {
	for i := db.config.ReplicaCount; i > 0; i-- {
		newOwners, err := db.consistent.GetClosestNForPartition(int(partID), i)
		if err == consistent.ErrInsufficientMemberCount {
			continue
		}
		if err != nil {
			// Fail early
			return nil, err
		}
		return newOwners, nil
	}
	return nil, consistent.ErrInsufficientMemberCount
}

func (db *Olric) distributeBackups(partID uint64) []discovery.Member {
	part := db.backups[partID]
	owners := make([]discovery.Member, part.ownerCount())
	copy(owners, part.loadOwners())

	newOwners, err := db.getReplicaOwners(partID)
	if err != nil {
		db.log.V(3).Printf("[ERROR] Failed to get replica owners for PartID: %d: %v",
			partID, err)
		return nil
	}

	// Remove the primary owner
	newOwners = newOwners[1:]

	// First run
	if len(owners) == 0 {
		for _, owner := range newOwners {
			owners = append(owners, owner.(discovery.Member))
		}
		return owners
	}

	// Prune dead nodes
	for i := 0; i < len(owners); i++ {
		backup := owners[i]
		cur, err := db.discovery.FindMemberByName(backup.Name)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to find %s in the cluster: %v", backup, err)
			// Delete it.
			owners = append(owners[:i], owners[i+1:]...)
			i--
			continue
		}
		if !hostCmp(backup, cur) {
			db.log.V(3).Printf("[WARN] One of the backup owners is probably re-joined: %s", cur)
			// Delete it.
			owners = append(owners[:i], owners[i+1:]...)
			i--
			continue
		}
	}

	// Prune empty nodes
	for i := 0; i < len(owners); i++ {
		backup := owners[i]
		req := protocol.NewSystemMessage(protocol.OpLengthOfPart)
		req.SetExtra(protocol.LengthOfPartExtra{
			PartID: partID,
			Backup: true,
		})
		res, err := db.requestTo(backup.String(), req)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to check key count on backup "+
				"partition: %d: %v", partID, err)
			// Pass it. If the node is down, memberlist package will send a leave event.
			continue
		}

		var count int32
		err = msgpack.Unmarshal(res.Value(), &count)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to unmarshal key count "+
				"while checking backup partition: %d: %v", partID, err)
			// This may be a temporary event. Pass it.
			continue
		}
		if count == 0 {
			// Delete it.
			db.log.V(5).Printf("[DEBUG] Empty backup partition found. PartID: %d on %s", partID, backup)
			owners = append(owners[:i], owners[i+1:]...)
			i--
		}
	}

	// Here add the new backup owners.
	for _, backup := range newOwners {
		var exists bool
		for i, bkp := range owners {
			if hostCmp(bkp, backup.(discovery.Member)) {
				exists = true
				// Remove it from the current position
				owners = append(owners[:i], owners[i+1:]...)
				// Append it again to head
				owners = append(owners, backup.(discovery.Member))
				break
			}
		}
		if !exists {
			owners = append(owners, backup.(discovery.Member))
		}
	}
	return owners
}

func (db *Olric) distributePrimaryCopies(partID uint64) []discovery.Member {
	// First you need to create a copy of the owners list. Don't modify the current list.
	part := db.partitions[partID]
	owners := make([]discovery.Member, part.ownerCount())
	copy(owners, part.loadOwners())

	// Find the new partition owner.
	newOwner := db.consistent.GetPartitionOwner(int(partID))

	// First run.
	if len(owners) == 0 {
		owners = append(owners, newOwner.(discovery.Member))
		return owners
	}

	// Prune dead nodes
	for i := 0; i < len(owners); i++ {
		owner := owners[i]
		current, err := db.discovery.FindMemberByName(owner.Name)
		if err != nil {
			db.log.V(4).Printf("[ERROR] Failed to find %s in the cluster: %v", owner, err)
			owners = append(owners[:i], owners[i+1:]...)
			i--
			continue
		}
		if !hostCmp(owner, current) {
			db.log.V(4).Printf("[WARN] One of the partitions owners is probably re-joined: %s", current)
			owners = append(owners[:i], owners[i+1:]...)
			i--
			continue
		}
	}

	// Prune empty nodes
	for i := 0; i < len(owners); i++ {
		owner := owners[i]
		req := protocol.NewSystemMessage(protocol.OpLengthOfPart)
		req.SetExtra(protocol.LengthOfPartExtra{PartID: partID})
		res, err := db.requestTo(owner.String(), req)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to check key count on partition: %d: %v", partID, err)
			// Pass it. If the node is gone, memberlist package will notify us.
			continue
		}

		var count int32
		err = msgpack.Unmarshal(res.Value(), &count)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to unmarshal key count "+
				"while checking primary partition: %d: %v", partID, err)
			// This may be a temporary issue.
			// Pass it. If the node is gone, memberlist package will notify us.
			continue
		}
		if count == 0 {
			db.log.V(6).Printf("[DEBUG] PartID: %d on %s is empty", partID, owner)
			// Empty partition. Delete it from ownership list.
			owners = append(owners[:i], owners[i+1:]...)
			i--
		}
	}

	// Here add the new partition newOwner.
	for i, owner := range owners {
		if hostCmp(owner, newOwner.(discovery.Member)) {
			// Remove it from the current position
			owners = append(owners[:i], owners[i+1:]...)
			// Append it again to head
			return append(owners, newOwner.(discovery.Member))
		}
	}
	return append(owners, newOwner.(discovery.Member))
}

// 生成新路由表，该路由表包含集群中所有分区及其对应的主副本和备份副本的分布信息。
// 这个函数遍历所有分区，并为每个分区分配主副本和备份副本。
func (db *Olric) distributePartitions() routingTable {
	table := make(routingTable)
	// 分区数由 db.config.PartitionCount 决定
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		// 获取当前分区 ID 对应的路由表项 item
		item := table[partID]
		// 为当前分区分配主副本
		item.Owners = db.distributePrimaryCopies(partID)
		// 为当前分区分配备份副本
		if db.config.ReplicaCount > config.MinimumReplicaCount { // 备份副本数
			item.Backups = db.distributeBackups(partID)
		}
		// 更新路由表项 item
		table[partID] = item
	}
	return table
}

func (db *Olric) updateRoutingTableOnCluster(table routingTable) (map[discovery.Member]ownershipReport, error) {
	data, err := msgpack.Marshal(table)
	if err != nil {
		return nil, err
	}

	var mtx sync.Mutex
	var g errgroup.Group
	ownershipReports := make(map[discovery.Member]ownershipReport)
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)
	for _, member := range db.consistent.GetMembers() {
		mem := member.(discovery.Member)
		g.Go(func() error {
			if err := sem.Acquire(db.ctx, 1); err != nil {
				db.log.V(3).Printf("[ERROR] Failed to acquire semaphore to update routing table on %s: %v", mem, err)
				return err
			}
			defer sem.Release(1)

			req := protocol.NewSystemMessage(protocol.OpUpdateRouting)
			req.SetValue(data)
			req.SetExtra(protocol.UpdateRoutingExtra{
				CoordinatorID: db.this.ID,
			})
			// TODO: This blocks whole flow. Use timeout for smooth operation.
			resp, err := db.requestTo(mem.String(), req)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to update routing table on %s: %v", mem, err)
				return err
			}

			ow := ownershipReport{}
			err = msgpack.Unmarshal(resp.Value(), &ow)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to call decode ownership report from %s: %v", mem, err)
				return err
			}
			mtx.Lock()
			ownershipReports[mem] = ow
			mtx.Unlock()

			return nil
		})
	}
	return ownershipReports, g.Wait()
}

func (db *Olric) updateRouting() {
	// This function is only run by the cluster coordinator.
	// 检查当前节点是否为集群的协调员，更新路由表只能由 coordinator 负责。
	if !db.discovery.IsCoordinator() {
		return
	}

	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	// 检查集群节点数量是否满足 Quorum 要求
	nr := atomic.LoadInt32(&db.numMembers)
	if db.config.MemberCountQuorum > nr {
		db.log.V(2).Printf("[ERROR] Impossible to calculate and update routing table: %v", ErrClusterQuorum)
		return
	}

	// This function is called by listenMemberlistEvents and updateRoutingPeriodically
	// So this lock prevents parallel execution.
	// 路由更新操作无法并行
	routingMtx.Lock()
	defer routingMtx.Unlock()

	// 计算每个分区的主副本和备份副本的所有者，生成最新路由表
	table := db.distributePartitions()
	// 将新路由表分发到集群中的所有节点
	reports, err := db.updateRoutingTableOnCluster(table)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to update routing table on cluster: %v", err)
	}
	// 从集群节点收集到的所有权报告。这一步确保每个节点上的数据分区和备份副本的所有权信息是最新的
	db.processOwnershipReports(reports)

}

func (db *Olric) processOwnershipReports(reports map[discovery.Member]ownershipReport) {
	check := func(member discovery.Member, owners []discovery.Member) bool {
		for _, owner := range owners {
			if hostCmp(member, owner) {
				return true
			}
		}
		return false
	}

	ensureOwnership := func(member discovery.Member, partID uint64, part *partition) {
		owners := part.loadOwners()
		if check(member, owners) {
			return
		}
		// This section is protected by routingMtx against parallel writers.
		//
		// Copy owners and append the member to head
		newOwners := make([]discovery.Member, len(owners))
		copy(newOwners, owners)
		// Prepend
		newOwners = append([]discovery.Member{member}, newOwners...)
		part.owners.Store(newOwners)
		db.log.V(2).Printf("[INFO] %s still have some data for PartID (backup:%v): %d", member, part.backup, partID)
	}

	// data structures in this function is guarded by routingMtx
	for member, report := range reports {
		for _, partID := range report.Partitions {
			part := db.partitions[partID]
			ensureOwnership(member, partID, part)
		}

		for _, partID := range report.Backups {
			part := db.backups[partID]
			ensureOwnership(member, partID, part)
		}
	}
}

// 根据 ml 事件更新 db.members, db.consistent 和 db.numMembers 。
func (db *Olric) processNodeEvent(event *discovery.ClusterEvent) {
	db.members.mtx.Lock()
	defer db.members.mtx.Unlock()

	// 解析 node meta
	member, _ := db.discovery.DecodeNodeMeta(event.NodeMeta)

	// 节点加入：
	//	- 将新节点添加到成员列表中 db.members.m 。
	//	- 将新节点添加到一致性哈希环 db.consistent 中。
	if event.Event == memberlist.NodeJoin {
		db.members.m[member.ID] = member
		db.consistent.Add(member)
		db.log.V(2).Printf("[INFO] Node joined: %s", member)
	} else if event.Event == memberlist.NodeLeave {
		// 节点离开：
		//	- 检查节点是否在成员列表中，如果存在则删除。
		//	- 从一致性哈希环中移除该节点。
		//	- 关闭与该节点相关的连接池，避免再次使用已关闭的套接字。
		if _, ok := db.members.m[member.ID]; ok {
			delete(db.members.m, member.ID)
		} else {
			db.log.V(2).Printf("[ERROR] Unknown node left: %s", event.NodeName)
			return
		}
		db.consistent.Remove(event.NodeName)
		// Don't try to used closed sockets again.
		db.client.ClosePool(event.NodeName)
		db.log.V(2).Printf("[INFO] Node left: %s", event.NodeName)
	} else {
		db.log.V(2).Printf("[ERROR] Unknown event received: %v", event)
		return
	}

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	//
	// 更新节点数目
	db.storeNumMembers()
}

// 监听 ml 事件，执行对应逻辑
func (db *Olric) listenMemberlistEvents(eventCh chan *discovery.ClusterEvent) {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			return
		case e := <-eventCh:
			db.processNodeEvent(e) // 根据 ml 事件更新 db.members, db.consistent 和 db.numMembers 。
			db.updateRouting()     //
		}
	}
}

func (db *Olric) updateRoutingPeriodically() {
	defer db.wg.Done()
	// TODO: Make this parametric.
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			db.updateRouting()
		}
	}
}

func (db *Olric) checkAndGetCoordinator(id uint64) (discovery.Member, error) {
	coordinator, err := db.discovery.FindMemberByID(id)
	if err != nil {
		return discovery.Member{}, err
	}

	myCoordinator := db.discovery.GetCoordinator()
	if !hostCmp(coordinator, myCoordinator) {
		return discovery.Member{}, fmt.Errorf("unrecognized cluster coordinator: %s: %s", coordinator, myCoordinator)
	}
	return coordinator, nil
}

func (db *Olric) setOwnedPartitionCount() {
	var count uint64
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		if hostCmp(part.owner(), db.this) {
			count++
		}
	}

	atomic.StoreUint64(&db.ownedPartitionCount, count)
}

func (db *Olric) updateRoutingOperation(w, r protocol.EncodeDecoder) {
	routingUpdateMtx.Lock()
	defer routingUpdateMtx.Unlock()

	req := r.(*protocol.SystemMessage)
	table := make(routingTable)
	err := msgpack.Unmarshal(req.Value(), &table)
	if err != nil {
		db.errorResponse(w, err)
		return
	}

	coordinatorID := req.Extra().(protocol.UpdateRoutingExtra).CoordinatorID
	coordinator, err := db.checkAndGetCoordinator(coordinatorID)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Routing table cannot be updated: %v", err)
		db.errorResponse(w, err)
		return
	}

	// Compare partition counts to catch a possible inconsistencies in configuration
	if db.config.PartitionCount != uint64(len(table)) {
		db.log.V(2).Printf("[ERROR] Routing table cannot be updated. "+
			"Expected partition count is %d, got: %d", db.config.PartitionCount, uint64(len(table)))
		db.errorResponse(w, ErrInvalidArgument)
		return
	}

	// owners(atomic.value) is guarded by routingUpdateMtx against parallel writers.
	// Calculate routing signature. This is useful to control rebalancing tasks.
	atomic.StoreUint64(&routingSignature, db.hasher.Sum64(req.Value()))
	for partID, data := range table {
		// Set partition(primary copies) owners
		part := db.partitions[partID]
		part.owners.Store(data.Owners)

		// Set backup owners
		bpart := db.backups[partID]
		bpart.owners.Store(data.Backups)
	}

	db.setOwnedPartitionCount()

	// Bootstrapped by the coordinator.
	atomic.StoreInt32(&db.bootstrapped, 1)
	// Collect report
	data, err := db.prepareOwnershipReport()
	if err != nil {
		db.errorResponse(w, ErrInvalidArgument)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(data)

	// Call rebalancer to rebalance partitions
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		db.rebalancer()

		// Clean stale dmaps
		db.deleteStaleDMaps()
	}()
	db.log.V(3).Printf("[INFO] Routing table has been pushed by %s", coordinator)
}

type ownershipReport struct {
	Partitions []uint64
	Backups    []uint64
}

func (db *Olric) prepareOwnershipReport() ([]byte, error) {
	res := ownershipReport{}
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		if part.length() != 0 {
			res.Partitions = append(res.Partitions, partID)
		}

		backup := db.backups[partID]
		if backup.length() != 0 {
			res.Backups = append(res.Backups, partID)
		}
	}
	return msgpack.Marshal(res)
}

func (db *Olric) keyCountOnPartOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.SystemMessage)
	partID := req.Extra().(protocol.LengthOfPartExtra).PartID
	isBackup := req.Extra().(protocol.LengthOfPartExtra).Backup

	var part *partition
	if isBackup {
		part = db.backups[partID]
	} else {
		part = db.partitions[partID]
	}

	value, err := msgpack.Marshal(part.length())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}
