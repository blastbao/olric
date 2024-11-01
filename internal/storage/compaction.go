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

package storage

import (
	"log"
)

// CompactTables
//
// Olric 的 Storage 结构包含多个表，每个表用于存储键值对。
// 随着数据的增删，某些表可能会变得零散，产生“碎片”。
// 该方法的目的是将旧表的数据合并到最新的表中，从而减少内存浪费并提高访问性能。
//
// 具体逻辑:
// 1. 如果只有一个表，说明没有必要进行压缩操作，直接返回 true。
//
// 2. 合并旧表数据:
//   - 获取最新的表 fresh ，fresh 表是最后添加的表，新数据会合并到这里
//   - 遍历所有旧表（除了最后一个表）中的键值对
//   - 对于每个哈希键，从旧表中获取其原始数据 (getRaw) 并将其放入 fresh 表中 (putRaw)
//
// 3. 如果 putRaw 返回 errNotEnoughSpace，说明当前表容量不足
//   - 新建一个表，容量为当前使用空间的两倍（Inuse() * 2）
//   - 将新表添加到 s.tables ，并立即退出 Compact 操作，返回 false 表示压缩尚未完成
//
// 4. 如果插入成功，则从旧表中删除该键值对以释放空间
// 5. 如果总处理的键值对数量超过 1000，则停止压缩并返回 false，以避免长时间阻塞操作
// 6. 遍历除 fresh 外的所有表，删除没有键的空表，只保留非空表和最后一个表
// 7. 压缩成功则返回 true，否则返回 false 表示压缩尚未完成
func (s *Storage) CompactTables() bool {
	if len(s.tables) == 1 {
		return true
	}

	var total int
	fresh := s.tables[len(s.tables)-1]
	for _, old := range s.tables[:len(s.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range old.hkeys {
			vdata, _ := old.getRaw(hkey)
			err := fresh.putRaw(hkey, vdata)
			if err == errNotEnoughSpace {
				// Create a new table and put the new k/v pair in it.
				nt := newTable(s.Inuse() * 2)
				s.tables = append(s.tables, nt)
				return false
			}
			if err != nil {
				log.Printf("[ERROR] Failed to compact tables. HKey: %d: %v", hkey, err)
			}

			// Dont check the returned val, it's useless because
			// we are sure that the key is already there.
			old.delete(hkey)
			total++
			if total > 1000 {
				// It's enough. Don't block the instance.
				return false
			}
		}
	}

	// Remove empty tables. Keep the last table.
	tmp := []*table{s.tables[len(s.tables)-1]}
	for _, t := range s.tables[:len(s.tables)-1] {
		if len(t.hkeys) == 0 {
			continue
		}
		tmp = append(tmp, t)
	}
	s.tables = tmp
	return true
}
