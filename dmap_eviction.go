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
	"math/rand"
	"runtime"
	"sort"
	"time"

	"github.com/buraksezer/olric/internal/storage"
	"golang.org/x/sync/semaphore"
)

// 启动协程来进行键的淘汰。使用信号量控制并发淘汰工作线程的数量，根据 CPU 数量或配置的值来决定。
func (db *Olric) evictKeysAtBackground() {
	defer db.wg.Done()

	num := int64(runtime.NumCPU())
	if db.config.Cache != nil && db.config.Cache.NumEvictionWorkers != 0 {
		num = db.config.Cache.NumEvictionWorkers
	}
	sem := semaphore.NewWeighted(num)
	for {
		if !db.isAlive() {
			return
		}

		if err := sem.Acquire(db.ctx, 1); err != nil {
			db.log.V(3).Printf("[ERROR] Failed to acquire semaphore: %v", err)
			return
		}

		db.wg.Add(1)
		go func() {
			defer db.wg.Done()
			defer sem.Release(1)
			// Good for developing tests.
			db.evictKeys()
			select {
			case <-time.After(100 * time.Millisecond):
			case <-db.ctx.Done():
				return
			}
		}()
	}
}

// 随机选择一个分区
func (db *Olric) evictKeys() {
	partID := uint64(rand.Intn(int(db.config.PartitionCount)))
	part := db.partitions[partID]
	part.m.Range(func(name, tmp interface{}) bool {
		dm := tmp.(*dmap)
		db.scanDMapForEviction(partID, name.(string), dm)
		// this breaks the loop, we only scan one dmap instance per call
		return false
	})
}

// 从 Redis 的文档中借鉴了策略：
//   - 随机测试 20 个有过期时间的键。
//   - 删除所有已过期的键。
//   - 如果超过 25% 的键已过期，重复上述步骤。
//
// 策略控制:
//
//	maxKeyCount: 每次最多检查 20 个键。
//	maxTotalCount: 总共最多处理 100 个键，防止 CPU 资源耗尽。
//
// janitor 函数：
//   - 遍历 dmap 的存储，检查每个键。
//   - 如果键过期或闲置，尝试删除。
//   - 删除失败时记录错误日志，并继续尝试下一个键。
//   - 统计删除的键数量。
//
// 循环控制：
//   - 每次尝试最多检查 20 个键，如果删除的键数量超过 25%（即 5 个），继续执行 janitor 。
//   - 如果总删除数量超过 100 个，停止扫描。
func (db *Olric) scanDMapForEviction(partID uint64, name string, dm *dmap) {
	/*
		From Redis Docs:
			1- Test 20 random keys from the set of keys with an associated expire.
			2- Delete all the keys found expired.
			3- If more than 25% of keys were expired, start again from step 1.
	*/

	// We need limits to prevent CPU starvation. delKeyVal does some network operation
	// to delete keys from the backup nodes and the previous owners.
	var maxKeyCount = 20
	var maxTotalCount = 100
	var totalCount = 0

	dm.Lock()
	defer dm.Unlock()

	janitor := func() bool {
		if totalCount > maxTotalCount {
			// Release the lock. Eviction will be triggered again.
			return false
		}

		count, keyCount := 0, 0
		dm.storage.Range(func(hkey uint64, vdata *storage.VData) bool {
			keyCount++
			if keyCount >= maxKeyCount {
				// this means 'break'.
				return false
			}
			if isKeyExpired(vdata.TTL) || dm.isKeyIdle(hkey) {
				err := db.delKeyVal(dm, hkey, name, vdata.Key)
				if err != nil {
					// It will be tried again.
					db.log.V(3).Printf("[ERROR] Failed to delete expired hkey: %d on dmap: %s: %v", hkey, name, err)
					return true // this means 'continue'
				}
				count++
			}
			return true
		})
		totalCount += count
		return count >= maxKeyCount/4
	}
	defer func() {
		if totalCount > 0 {
			if db.log.V(6).Ok() {
				db.log.V(6).Printf("[DEBUG] Evicted key count is %d on PartID: %d", totalCount, partID)
			}
		}
	}()
	for {
		select {
		case <-db.ctx.Done():
			// The server has gone.
			return
		default:
		}
		// Call janitor again until it returns false.
		if !janitor() {
			return
		}
	}
}

// 更新 hkey 的最近访问时间
func (dm *dmap) updateAccessLog(hkey uint64) {
	if dm.cache == nil || dm.cache.accessLog == nil {
		// Fail early. This's useful to avoid checking the configuration everywhere.
		return
	}
	dm.cache.Lock()
	defer dm.cache.Unlock()
	dm.cache.accessLog[hkey] = time.Now().UnixNano()
}

func (dm *dmap) deleteAccessLog(hkey uint64) {
	if dm.cache == nil || dm.cache.accessLog == nil {
		return
	}
	dm.cache.Lock()
	defer dm.cache.Unlock()
	delete(dm.cache.accessLog, hkey)
}

func (dm *dmap) isKeyIdle(hkey uint64) bool {
	if dm.cache == nil {
		return false
	}
	if dm.cache.accessLog == nil || dm.cache.maxIdleDuration.Nanoseconds() == 0 {
		return false
	}
	// Maximum time in seconds for each entry to stay idle in the map.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period
	// exceeds this limit are expired and evicted automatically.
	dm.cache.RLock()
	defer dm.cache.RUnlock()
	t, ok := dm.cache.accessLog[hkey]
	if !ok {
		return false
	}
	ttl := (dm.cache.maxIdleDuration.Nanoseconds() + t) / 1000000
	return isKeyExpired(ttl)
}

type lruItem struct {
	HKey       uint64
	AccessedAt int64
}

func (db *Olric) evictKeyWithLRU(dm *dmap, name string) error {
	idx := 1
	items := []lruItem{}
	dm.cache.RLock()
	// Pick random items from the distributed map and sort them by accessedAt.
	for hkey, accessedAt := range dm.cache.accessLog {
		if idx >= dm.cache.lruSamples {
			break
		}
		idx++
		i := lruItem{
			HKey:       hkey,
			AccessedAt: accessedAt,
		}
		items = append(items, i)
	}
	dm.cache.RUnlock()

	if len(items) == 0 {
		return fmt.Errorf("nothing found to expire with LRU")
	}
	sort.Slice(items, func(i, j int) bool { return items[i].AccessedAt < items[j].AccessedAt })
	// Pick the first item to delete. It's the least recently used item in the sample.
	item := items[0]
	key, err := dm.storage.GetKey(item.HKey)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			err = ErrKeyNotFound
		}
		return err
	}
	if db.log.V(6).Ok() {
		db.log.V(6).Printf("[DEBUG] Evicted item on dmap: %s, key: %s with LRU", name, key)
	}
	return db.delKeyVal(dm, item.HKey, name, key)
}
