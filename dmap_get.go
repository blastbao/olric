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
	"errors"
	"sort"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/vmihailenco/msgpack"
)

var ErrReadQuorum = errors.New("read quorum cannot be reached")

type version struct {
	host *discovery.Member
	data *storage.VData
}

func (db *Olric) unmarshalValue(rawval []byte) (interface{}, error) {
	var value interface{}
	err := db.serializer.Unmarshal(rawval, &value)
	if err != nil {
		return nil, err
	}
	// 对空值的特殊处理(把 struct{} 序列化存储)，标识数据不存在
	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}

// 函数 lookupOnOwners 在分区所有者及其之前的所有者中收集一个键/值对的所有版本。
// 数据可能会被分配到不同的主机上（例如因分区重分配或复制策略变化时），函数会查找当前和以前的分区拥有者，以确保完整收集到该键的所有版本。
//
// 这个函数的主要目的是确保在分布式环境中，能够获取到某个键的所有可能版本，以便进行一致性检查或数据恢复。
// 通过查询当前和之前的所有者，可以最大限度地收集到完整的数据历史。
//
// 步骤：
//   - 本地主机检查: 首先在本地主机（当前分区所有者）上查找键值对。
//   - 版本收集: 将找到的版本存储在 versions 列表中。
//   - 查询前任所有者: 如果当前所有者没有找到，或者需要收集所有版本，则查询之前的分区所有者。
//
// lookupOnOwners collects versions of a key/value pair on the partition owner
// by including previous partition owners.
func (db *Olric) lookupOnOwners(dm *dmap, hkey uint64, name, key string) []*version {
	var versions []*version

	// Check on localhost, the partition owner.
	value, err := dm.storage.Get(hkey)
	if err != nil {
		// still need to use "ver". just log this error.
		if err == storage.ErrKeyNotFound {
			// the requested key can be found on a replica or a previous partition owner.
			if db.log.V(5).Ok() {
				db.log.V(5).Printf("[DEBUG] key: %s, HKey: %d on dmap: %s could not be found on the local storage: %v", key, hkey, name, err)
			}
		} else {
			db.log.V(3).Printf("[ERROR] Failed to get key: %s on %s could not be found: %s", key, name, err)
		}
	}

	ver := &version{
		host: &db.this,
		data: value,
	}
	versions = append(versions, ver)

	// Run a query on the previous owners.
	owners := db.getPartitionOwners(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		req := protocol.NewDMapMessage(protocol.OpGetPrev)
		req.SetDMap(name)
		req.SetKey(key)

		ver := &version{host: &owner}
		resp, err := db.requestTo(owner.String(), req)
		if err != nil {
			if db.log.V(3).Ok() {
				db.log.V(3).Printf("[ERROR] Failed to call get on a previous "+
					"primary owner: %s: %v", owner, err)
			}
		} else {
			data := storage.VData{}
			err = msgpack.Unmarshal(resp.Value(), data)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to unmarshal data from the "+
					"previous primary owner: %s: %v", owner, err)
			} else {
				ver.data = &data
				// Ignore failed owners. The data on those hosts will be wiped out by the rebalancer.
				versions = append(versions, ver)
			}
		}
	}

	return versions
}

// 按照 Timestamp 降序排列，使得最新的版本位于切片的开头。
func (db *Olric) sortVersions(versions []*version) []*version {
	sort.Slice(versions,
		func(i, j int) bool {
			return versions[i].data.Timestamp >= versions[j].data.Timestamp
		},
	)
	// Explicit is better than implicit.
	return versions
}

// 清除数据为 nil 的版本，然后按照 Timestamp 排序
func (db *Olric) sanitizeAndSortVersions(versions []*version) []*version {
	var sanitized []*version
	// We use versions slice for read-repair. Clear nil values first.
	for _, ver := range versions {
		if ver.data != nil {
			sanitized = append(sanitized, ver)
		}
	}
	if len(sanitized) <= 1 {
		return sanitized
	}
	return db.sortVersions(sanitized)
}

func (db *Olric) lookupOnReplicas(hkey uint64, name, key string) []*version {
	var versions []*version
	// Check backups.
	backups := db.getBackupPartitionOwners(hkey)
	for _, replica := range backups {
		req := protocol.NewDMapMessage(protocol.OpGetBackup)
		req.SetDMap(name)
		req.SetKey(key)
		ver := &version{host: &replica}
		resp, err := db.requestTo(replica.String(), req)
		if err != nil {
			if db.log.V(3).Ok() {
				db.log.V(3).Printf("[ERROR] Failed to call get on a replica owner: %s: %v", replica, err)
			}
		} else {
			value := storage.VData{}
			err = msgpack.Unmarshal(resp.Value(), &value)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to unmarshal data from a replica owner: %s: %v", replica, err)
			} else {
				ver.data = &value
			}
		}
		versions = append(versions, ver)
	}
	return versions
}

// 函数 readRepair 用于在读取过程中修复不一致的数据版本，它确保集群中的所有副本都持有最新的、一致的数据。
//
// 遍历版本列表，跳过与最新版本（winner）时间戳相同的版本，因为这些版本已经是最新的，无需修复；
// 构造请求消息，包含 winner 版本数据 <key,value,ttl(opt),timestamp>
// 如果需要同步的旧版本是本地版本，就调用 localPut 来更新；如果是其他节点，就通过 rpc 发送 Put 请求；
func (db *Olric) readRepair(name string, dm *dmap, winner *version, versions []*version) {
	for _, ver := range versions {
		if ver.data != nil && winner.data.Timestamp == ver.data.Timestamp {
			continue
		}

		// If readRepair is enabled, this function is called by every GET request.
		var req *protocol.DMapMessage
		if winner.data.TTL == 0 {
			req = protocol.NewDMapMessage(protocol.OpPutReplica)
			req.SetDMap(name)
			req.SetKey(winner.data.Key)
			req.SetValue(winner.data.Value)
			req.SetExtra(protocol.PutExtra{Timestamp: winner.data.Timestamp})
		} else {
			req = protocol.NewDMapMessage(protocol.OpPutExReplica)
			req.SetDMap(name)
			req.SetKey(winner.data.Key)
			req.SetValue(winner.data.Value)
			req.SetExtra(protocol.PutExExtra{
				Timestamp: winner.data.Timestamp,
				TTL:       winner.data.TTL,
			})
		}

		// Sync
		if hostCmp(*ver.host, db.this) {
			hkey := db.getHKey(name, winner.data.Key)
			w := &writeop{
				dmap:      name,
				key:       winner.data.Key,
				value:     winner.data.Value,
				timestamp: winner.data.Timestamp,
				timeout:   time.Duration(winner.data.TTL),
			}
			dm.Lock()
			err := db.localPut(hkey, dm, w)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to synchronize with replica: %v", err)
			}
			dm.Unlock()
		} else {
			_, err := db.requestTo(ver.host.String(), req)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to synchronize replica %s: %v", ver.host, err)
			}
		}
	}
}

// [重要]
// 获取 dm 实例
// 给 dm 加读锁，注意这里没有用 defer 解锁，因为在某些情况下需要在 readRepair 中调用 localPut ，而 localPut 需要写锁
// 查找 table-key 在当前和之前拥有者上的所有版本
// 如果 ReadQuorum 大于 0 ，则查找副本版本，并添加到版本列表中
// 检查找到的版本数是否小于 ReadQuorum ，是则说明一致性不足，返回 ErrReadQuorum 错误
// 清除 versions 中数据为 nil 的版本，然后按照 Timestamp 排序，若清理后为空，说明在所有分区拥有者和副本上都没有找到数据，返回 ErrKeyNotFound
// 检查清理后版本数是否小于 ReadQuorum ，是则说明一致性不足，返回 ErrReadQuorum 错误
// 选择排序后的第一个版本(最新版本)，检查数据是否过期或 key 为闲置键，如果是，报错返回 ErrKeyNotFound
// 更新键的访问日志，便于维护数据的 LRU（最近最少使用）和 MaxIdleDuration（最大空闲持续时间）清理策略
// 如果配置启用了读修复，调用 db.readRepair() 进行版本同步
// 返回找到的最新版本的值
func (db *Olric) callGetOnCluster(hkey uint64, table, key string) ([]byte, error) {
	dm, err := db.getDMap(table, hkey)
	if err != nil {
		return nil, err
	}

	dm.RLock()
	// RUnlock should not be called with defer statement here because
	// readRepair function may call localPut function which needs a write
	// lock. Please don't forget calling RUnlock before returning here.

	versions := db.lookupOnOwners(dm, hkey, table, key)
	if db.config.ReadQuorum >= config.MinimumReplicaCount {
		v := db.lookupOnReplicas(hkey, table, key)
		versions = append(versions, v...)
	}
	if len(versions) < db.config.ReadQuorum {
		dm.RUnlock()
		return nil, ErrReadQuorum
	}

	sorted := db.sanitizeAndSortVersions(versions)
	if len(sorted) == 0 {
		// We checked everywhere, it's not here.
		dm.RUnlock()
		return nil, ErrKeyNotFound
	}
	if len(sorted) < db.config.ReadQuorum {
		dm.RUnlock()
		return nil, ErrReadQuorum
	}

	// The most up-to-date version of the values.
	winner := sorted[0]
	if isKeyExpired(winner.data.TTL) || dm.isKeyIdle(hkey) {
		dm.RUnlock()
		return nil, ErrKeyNotFound
	}

	// LRU and MaxIdleDuration eviction policies are only valid on
	// the partition owner. Normally, we shouldn't need to retrieve the keys
	// from the backup or the previous owners. When the fsck merge
	// a fragmented partition or recover keys from a backup, Olric
	// continue maintaining a reliable access log.
	dm.updateAccessLog(hkey)

	dm.RUnlock()
	if db.config.ReadRepair {
		// Parallel read operations may propagate different versions of
		// the same key/value pair. The rule is simple: last write wins.
		db.readRepair(table, dm, winner, versions)
	}
	return winner.data.Value, nil
}

// 根据 name + key 的 hash % partCount 定位到 part ，找到 part owner；
// 如果 owner 是本机，直接本地读取；否则，重定向请求到 owner 。
func (db *Olric) get(table, key string) ([]byte, error) {
	member, hkey := db.findPartitionOwner(table, key)
	// We are on the partition owner
	if hostCmp(member, db.this) {
		return db.callGetOnCluster(hkey, table, key)
	}
	// Redirect to the partition owner
	req := protocol.NewDMapMessage(protocol.OpGet)
	req.SetDMap(table)
	req.SetKey(key)
	resp, err := db.requestTo(member.String(), req)
	if err != nil {
		return nil, err
	}
	return resp.Value(), nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contains the key. It's thread-safe. It is safe to modify the contents
// of the returned value. It is safe to modify the contents of the argument
// after Get returns.
func (dm *DMap) Get(key string) (interface{}, error) {
	rawval, err := dm.db.get(dm.name, key)
	if err != nil {
		return nil, err
	}
	return dm.db.unmarshalValue(rawval)
}

func (db *Olric) exGetOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	value, err := db.get(req.DMap(), req.Key())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

// 在 Olric 中，getBackupOperation 是用于处理备份节点上的读取操作的函数。
//
// 作用
//   - 读取备份数据: 当主节点不可用或需要从备份节点读取数据时，getBackupOperation 会被调用。
//   - 确保数据可用性: 在分布式系统中，数据通常会存储多个副本。getBackupOperation 允许从备份节点读取数据，从而提高系统的可用性和容错能力。
//
// 何时使用
//   - 主节点故障: 如果主节点出现故障，系统可以从备份节点读取数据，以确保数据的可用性。
//   - 负载均衡: 在某些情况下，可能会从备份节点读取数据以分散负载。
//   - 数据一致性检查: 用于确保备份节点上的数据与主节点一致。
//
// 通过这种方式，Olric 提供了更高的容错能力和数据可用性，确保在各种故障情况下依然能够正常读取数据。
//
// 步骤
//   - 解析请求：table 和 key
//   - 取出备份数据表 dm
//   - 从 dm 中读取 value ，判断是否过期
//   - 将数据 value 序列化并返回
func (db *Olric) getBackupOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := db.getHKey(req.DMap(), req.Key())
	dm, err := db.getBackupDMap(req.DMap(), hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	dm.RLock()
	defer dm.RUnlock()
	vdata, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	if isKeyExpired(vdata.TTL) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}

	value, err := msgpack.Marshal(*vdata)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

// 在 Olric 中，getPrevOperation 是一种操作，用于从之前的分区所有者处获取数据。
// 由于 Olric 是一个分布式内存缓存系统，分区的所有者可能会随着集群拓扑的改变而发生变化，例如在节点加入或离开集群时，分区的主节点可能会重新分配。
// getPrevOperation 可以用来在这些变化期间，向之前的分区所有者查询数据，确保数据的一致性和可用性。
//
// 主要作用：
//   - 访问旧分区所有者的数据：当数据无法在当前分区所有者上找到时，可以向旧的分区所有者发送 getPrevOperation 请求，以获取数据。
//   - 支持数据迁移或恢复：当分区所有权发生变化时，如果新主节点暂时缺少数据，getPrevOperation 可以作为一种临时的解决方案，从旧节点获取所需数据。
//   - 确保读一致性：在读取操作中，如果数据丢失或未找到，getPrevOperation 可以提供回退机制，确保即使在分区转移过程中，数据的读取请求也不会受到影响。
//
// 典型场景：
//   - 分区重新分配期间的数据恢复：当一个节点加入或离开集群，导致分区重新分配时，新节点可能需要从旧分区所有者获取原有数据。在这种情况下，getPrevOperation 会被调用，以查询旧的分区所有者。
//   - 读修复（Read Repair）过程：当一个数据请求因分区转移而无法从当前所有者找到数据时，可以通过 getPrevOperation 从先前的主节点查询数据。Olric 会通过这种方式尝试从旧分区所有者处获取最新版本的数据，来完成读修复。
//   - 数据一致性检查：在分区变更期间，集群中的某些副本可能会尝试从旧主节点获取数据，以检查数据一致性，确保数据在新旧分区所有者之间的一致性。
func (db *Olric) getPrevOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := db.getHKey(req.DMap(), req.Key())
	part := db.getPartition(hkey)
	tmp, ok := part.m.Load(req.DMap())
	if !ok {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}
	dm := tmp.(*dmap)

	vdata, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}

	if isKeyExpired(vdata.TTL) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}

	value, err := msgpack.Marshal(*vdata)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}
