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

// getReplicaOwners 它通过一致性哈希（consistent）算法找到分区最近的 N 个节点作为副本节点，
// 这些 “最近的 N 个节点” 既可以是主节点也可以是备份节点，通常第一个作为主节点，其余作为备份节点。
//
// 一致性哈希的基本原理
//   - 哈希环：
//     一致性哈希算法将所有节点和数据分区映射到一个固定大小的哈希空间（通常表现为一个环形的哈希环）。
//     每个节点被哈希到一个或多个位置上，而每个分区也映射到哈希环的某个位置。
//   - 最近节点分配：
//     数据或分区映射到环上后，通常将数据指派给离其位置最近的节点。
//     例如，数据分区在顺时针方向上会找到第一个节点作为“所有者”，以便读写操作可以路由到该节点。
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

// 管理和更新分区的备份副本节点列表，以确保备份分布在可用且最新的节点上，同时移除失效或不再包含有效数据的节点。
//
// 步骤：
// - 获取当前备份节点列表：获取当前分区 partID 的所有者列表，并创建其副本以避免直接修改。
// - 获取新的备份节点列表：调用 getReplicaOwners 方法获取分区 partID 新的副本列表，移除其中的主所有者（即 newOwners[0]），因为该函数只关注备份所有者。
// - 如果分区没有备份所有者，直接将新的备份所有者添加到列表中并返回。
// - 移除失效节点：每个备份节点 backup 是否仍存在于集群，且身份信息未发生变更。
// - 移除空节点：检查备份节点上的数据是否为空。
// - 添加新的备份节点：将每个新的备份节点添加到 owners ，如果已经存在则移动到末尾，否则直接 append 。
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

// [重要]
//
// 更新分区的主副本拥有者列表，以确保数据的主副本在合适的节点上，并移除失效或空的节点。
// 1. 获取分区拥有者列表：从当前分区 part 的 owners 拷贝出一份副本。这样可以在不修改原始列表的情况下，对分区拥有者列表进行筛选和调整。
// 2. 获取新的分区主副本节点：使用一致性哈希算法找到新的分区所有者，通过 GetPartitionOwner 获取 partID 分区的新的主副本节点 newOwner 。
// 3. 首次分配：如果 owners 为空（即没有任何主副本拥有者），则将 newOwner 直接添加到 owners 中并返回。
// 4. 移除失效节点：循环遍历 owners，检查节点的有效性并移除失效的节点；indMemberByName 会通过 discovery 模块查找当前 owner 的节点是否存在于集群中。
// 5. 移除空节点：对每个 owner 发送 OpLengthOfPart 请求，获取该 partID 分区上键值对数量。如果分区为空 (count == 0)，说明该节点对该分区不再持有数据，将其从 owners 列表中移除。
// 6. 将新的主副本节点加入 owners ：如果 newOwner 已经在 owners 中，将其移动到列表末尾，当作最新主节点，否则直接 append 。
// 7. 返回最新的主副本节点列表。
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

// 构造空路由表
//
// 遍历分区，逐个分区填充其路由表项
//   - 获取分区路由
//   - 设置主副本
//   - 设置备份副本(如果指定需要备份副本)
//   - 更新分区路由
//
// 返回已填充路由表
func (db *Olric) distributePartitions() routingTable {
	table := make(routingTable)
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		item := table[partID]
		item.Owners = db.distributePrimaryCopies(partID)
		if db.config.ReplicaCount > config.MinimumReplicaCount {
			item.Backups = db.distributeBackups(partID)
		}
		table[partID] = item
	}
	return table
}

// 将路由表 table 序列化，以便通过 rpc 向集群各节点发送
// 构造 UpdateRouting 系统消息，并行发送给集群中各个节点，由信号量 sem 控制并行度不超过系统核数
// 逐个接收集群中各个节点返回的响应 `ownershipReport` ，其包含了该节点上有效的分区列表
// 汇总各个节点的 ownershipReports 并返回
func (db *Olric) updateRoutingTableOnCluster(table routingTable) (map[discovery.Member]ownershipReport, error) {
	data, err := msgpack.Marshal(table)
	if err != nil {
		return nil, err
	}

	var mtx sync.Mutex
	ownershipReports := make(map[discovery.Member]ownershipReport)

	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)
	var g errgroup.Group
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

// 检查当前节点是否为集群的协调员，更新路由表只能由 coordinator 负责
// 检查集群节点数量是否满足 Quorum 要求，如果不满足则不能更新路由表
// 路由更新操作无法并行，加锁保护
// [重要] 计算每个分区的主副本和备份副本，生成最新路由表
// 将新路由表分发到集群中的所有节点，并将各节点返回的 report 汇总起来，report 包含该节点上有效的分区列表
// [重要] 根据各节点上的分区信息更新本地路由表，确保 coordinator 包含最新最完整的分区路由信息，下次会继续广播给集群
func (db *Olric) updateRouting() {
	// This function is only run by the cluster coordinator.
	if !db.discovery.IsCoordinator() {
		return
	}

	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	nr := atomic.LoadInt32(&db.numMembers)
	if db.config.MemberCountQuorum > nr {
		db.log.V(2).Printf("[ERROR] Impossible to calculate and update routing table: %v", ErrClusterQuorum)
		return
	}

	// This function is called by listenMemberlistEvents and updateRoutingPeriodically
	// So this lock prevents parallel execution.
	routingMtx.Lock()
	defer routingMtx.Unlock()

	table := db.distributePartitions()
	reports, err := db.updateRoutingTableOnCluster(table)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to update routing table on cluster: %v", err)
	}

	db.processOwnershipReports(reports)
}

// processOwnershipReports 汇总每个成员的分区信息到本机的路由表中；
//
// check 用于检查 member 是否在 owners 列表中
// ensureOwnership 检查 member 是否是 part 的 owner ，如果不是则将 member 添加到 part 的 owner 列表头部
//
// 遍历每个 member 返回的 report ，其中包含该 member 上有效的 parts 和 backup parts ；
//   - 遍历 member 的 parts
//   - 如果 member 不存在于本地 db.partitions[partID] 的 owner 列表中，就添加进去；
//   - 如果 member 不存在于本地 db.backups[partID] 的 owner 列表中，就添加进去；
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

// processNodeEvent 根据 ml 事件更新 db.members, db.consistent 和 db.numMembers 。
//
// 1. 解析事件节点的 node meta
//
// 2. 节点加入：
//   - 将新节点添加到成员列表中 db.members.m 。
//   - 将新节点添加到一致性哈希环 db.consistent 中。
//
// 3. 节点离开：
//   - 检查节点是否在成员列表中，如果存在则删除。
//   - 从一致性哈希环中移除该节点。
//   - 关闭与该节点相关的连接池，避免再次使用已关闭的套接字。
//
// 4. 获取当前集群成员的数量存储到 db.numMembers 中
func (db *Olric) processNodeEvent(event *discovery.ClusterEvent) {
	db.members.mtx.Lock()
	defer db.members.mtx.Unlock()

	member, _ := db.discovery.DecodeNodeMeta(event.NodeMeta)

	if event.Event == memberlist.NodeJoin {
		db.members.m[member.ID] = member
		db.consistent.Add(member)
		db.log.V(2).Printf("[INFO] Node joined: %s", member)
	} else if event.Event == memberlist.NodeLeave {
		if _, ok := db.members.m[member.ID]; ok {
			delete(db.members.m, member.ID)
		} else {
			db.log.V(2).Printf("[ERROR] Unknown node left: %s", event.NodeName)
			return
		}
		db.consistent.Remove(event.NodeName)
		// Don't try to used closed sockets again.
		db.client.ClosePool(event.NodeName) // 关闭相关的连接池，释放资源
		db.log.V(2).Printf("[INFO] Node left: %s", event.NodeName)
	} else {
		db.log.V(2).Printf("[ERROR] Unknown event received: %v", event)
		return
	}

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	db.storeNumMembers()
}

// 监听 ml 事件，执行对应逻辑
//
// 备注：
//   - 只要发生 ml 事件，就意味着路由表更新，coordinator 接收到之后会广播路由变更消息
func (db *Olric) listenMemberlistEvents(eventCh chan *discovery.ClusterEvent) {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done(): // 超时
			return
		case e := <-eventCh: // ml 事件
			db.processNodeEvent(e) // 根据 ml 事件(join/leave)更新 db.members, db.consistent 和 db.numMembers 。
			db.updateRouting()     // 更新 partition 路由表
		}
	}
}

// 每分钟更新一次路由表并广播到 ml 集群，只有 Coordinator 有权广播
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

// 1. 从 ml 集群取出 id 节点 member
// 2. 从 ml 集群中取出启动时间最早的节点，它是 coordinator
// 3. 比较二者是否是同一个节点，若不同则报错，否则返回 member
func (db *Olric) checkAndGetCoordinator(id uint64) (discovery.Member, error) {
	member, err := db.discovery.FindMemberByID(id)
	if err != nil {
		return discovery.Member{}, err
	}
	coordinator := db.discovery.GetCoordinator()
	if !hostCmp(member, coordinator) {
		return discovery.Member{}, fmt.Errorf("unrecognized cluster coordinator: %s: %s", member, coordinator)
	}
	return member, nil
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

// 处理路由更新消息
//   - 解析 `路由更新` 系统消息，得到 RouteTable 和 CoordinatorID ，只有 Coordinator 可以发布路由更新消息；
//   - 根据 CoordinatorID 从 ml 集群中获取协调者，如果找不到或者不匹配则报错，以此确保消息来源合法；
//   - 检查路由表中的路由表项总数等于分区数，不一致则报错（要求路由表包含每个分区的路由）
//   - 计算 hash(RouteTable) 作为 routingSignature
//   - 遍历 RouteTable 中每个 <part, route> ，更新到本地路由表 db.partitions/db.backups 上
//   - 计算属于本节点的主分区总数，存储到 db.ownedPartitionCount
//   - 获取本节点非空的主分区和备份分区 PartId 列表，存入 data 中返回给调用者
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
		db.log.V(2).Printf("[ERROR] Routing table cannot be updated. "+"Expected partition count is %d, got: %d", db.config.PartitionCount, uint64(len(table)))
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

// 包含非空的主分区和备份分区 PartId 列表
type ownershipReport struct {
	Partitions []uint64
	Backups    []uint64
}

// 遍历每个 part ，如果当前节点中其主分区非空，就将 PartID 存入 res.Partitions ，同理备份分区；
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
