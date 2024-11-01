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
	"encoding/binary"

	"github.com/pkg/errors"
)

// 最大键长度为 256 字节
const maxKeyLen = 256

var (
	// 内存空间不足
	errNotEnoughSpace = errors.New("not enough space")
	// ErrKeyTooLarge is an error that indicates the given key is large than the determined key size.
	// The current maximum key length is 256.
	// 键长度超过最大限制
	ErrKeyTooLarge = errors.New("key too large")
	// ErrKeyNotFound is an error that indicates that the requested key could not be found in the DB.
	// 键未找到
	ErrKeyNotFound = errors.New("key not found")
)

type table struct {
	hkeys  map[uint64]int // 存储键哈希值到内存偏移量的映射
	memory []byte         // 实际存储数据
	offset int            // 当前内存使用的偏移量

	// In bytes
	allocated int // 已分配的内存大小
	inuse     int // 当前使用的内存大小
	garbage   int // 标记为垃圾的内存大小
}

func newTable(size int) *table {
	if size < minimumSize {
		size = minimumSize
	}
	t := &table{
		hkeys:     make(map[uint64]int),
		allocated: size,
	}
	//  From builtin.go:
	//
	//  The size specifies the length. The capacity of the slice is
	//	equal to its length. A second integer argument may be provided to
	//	specify a different capacity; it must be no smaller than the
	//	length. For example, make([]int, 0, 10) allocates an underlying array
	//	of size 10 and returns a slice of length 0 and capacity 10 that is
	//	backed by this underlying array.
	t.memory = make([]byte, size)
	return t
}

func (t *table) putRaw(hkey uint64, value []byte) error {
	// Check empty space on allocated memory area.
	inuse := len(value)
	if inuse+t.offset >= t.allocated {
		return errNotEnoughSpace
	}
	t.hkeys[hkey] = t.offset
	copy(t.memory[t.offset:], value)
	t.inuse += inuse
	t.offset += inuse
	return nil
}

// In-memory layout for entry:
// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | | Timestamp(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
//
// 内存布局：
//
//	+------------+-----------+-----------+--------------+-----------------+--------------+
//	| KEY-LENGTH |   KEY     |    TTL    |  Timestamp   | VALUE-LENGTH    |    VALUE     |
//	|  (1 byte)  | (n bytes) | (8 bytes) |  (8 bytes)   |   (4 bytes)     |  (m bytes)   |
//	+------------+-----------+-----------+--------------+-----------------+--------------+
//
// put 过程:
//
// 键长度检查: 如果键的长度超过 maxKeyLen（256 字节），返回 ErrKeyTooLarge 错误。
// 内存空间检查: 如果所需空间超过已分配的内存，返回 errNotEnoughSpace 错误。
// 删除已有键: 检查键是否已存在，如果存在，先删除旧数据。
// 更新哈希映射和内存使用:
// 如果成功存储，返回 nil。
func (t *table) put(hkey uint64, value *VData) error {
	if len(value.Key) >= maxKeyLen {
		return ErrKeyTooLarge
	}

	// Check empty space on allocated memory area.
	inuse := len(value.Key) + len(value.Value) + 21 // TTL + Timestamp + value-Length + key-Length
	if inuse+t.offset >= t.allocated {
		return errNotEnoughSpace
	}

	// If we already have the key, delete it.
	if _, ok := t.hkeys[hkey]; ok {
		t.delete(hkey)
	}

	t.hkeys[hkey] = t.offset
	t.inuse += inuse

	// Set key length. It's 1 byte.
	klen := uint8(len(value.Key))
	copy(t.memory[t.offset:], []byte{klen})
	t.offset++

	// Set the key.
	copy(t.memory[t.offset:], value.Key)
	t.offset += len(value.Key)

	// Set the TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.TTL))
	t.offset += 8

	// Set the Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.Timestamp))
	t.offset += 8

	// Set the value length. It's 4 bytes.
	binary.BigEndian.PutUint32(t.memory[t.offset:], uint32(len(value.Value)))
	t.offset += 4

	// Set the value.
	copy(t.memory[t.offset:], value.Value)
	t.offset += len(value.Value)
	return nil
}

func (t *table) getRaw(hkey uint64) ([]byte, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}
	start, end := offset, offset

	// In-memory structure:
	// 1                 | klen       | 8           | 8                  | 4                    | vlen
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | Timestamp(uint64)  | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := int(t.memory[end])
	end++       // One byte to keep key length
	end += klen // key length
	end += 8    // For bytes for TTL
	end += 8    // For bytes for Timestamp

	vlen := binary.BigEndian.Uint32(t.memory[end : end+4])
	end += 4         // 4 bytes to keep value length
	end += int(vlen) // value length

	// Create a copy of the requested data.
	rawval := make([]byte, (end-start)+1)
	copy(rawval, t.memory[start:end])
	return rawval, false
}

func (t *table) getRawKey(hkey uint64) ([]byte, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}

	klen := int(t.memory[offset])
	offset++
	return t.memory[offset : offset+klen], false
}

func (t *table) getKey(hkey uint64) (string, bool) {
	raw, prev := t.getRawKey(hkey)
	if raw == nil {
		return "", prev
	}
	return string(raw), prev
}

func (t *table) getTTL(hkey uint64) (int64, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return 0, true
	}

	klen := int(t.memory[offset])
	offset++
	offset += klen
	ttl := int64(binary.BigEndian.Uint64(t.memory[offset : offset+8]))
	return ttl, false
}

func (t *table) get(hkey uint64) (*VData, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}

	vdata := &VData{}
	// In-memory structure:
	//
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | Timestamp(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := int(uint8(t.memory[offset]))
	offset++

	vdata.Key = string(t.memory[offset : offset+klen])
	offset += klen

	vdata.TTL = int64(binary.BigEndian.Uint64(t.memory[offset : offset+8]))
	offset += 8

	vdata.Timestamp = int64(binary.BigEndian.Uint64(t.memory[offset : offset+8]))
	offset += 8

	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	offset += 4
	vdata.Value = t.memory[offset : offset+int(vlen)]
	return vdata, false
}

// 功能
// delete 方法用于从内存中删除指定的键值对，并更新相关的元数据。
//
// 步骤
// 查找给定 hkey 的偏移量，如果找不到，返回 true ，表示键可能在前一个表中
// 从 offset 开始，计算当前 hkey 对应 value 占用的空间，删除后这部分空间变为垃圾空间
// 从 hkeys 中删除该键
// 将删除的数据大小加到 garbage 中
// 减少 inuse 的内存使用
// 返回 false，表示成功删除
func (t *table) delete(hkey uint64) bool {
	offset, ok := t.hkeys[hkey]
	if !ok {
		// Try the previous table.
		return true
	}
	var garbage int

	// key, 1 byte for key size, klen for key's actual length.
	klen := int(uint8(t.memory[offset]))
	offset += 1 + klen
	garbage += 1 + klen

	// TTL, skip it.
	offset += 8
	garbage += 8

	// Timestamp, skip it.
	offset += 8
	garbage += 8

	// value len and its header.
	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	garbage += 4 + int(vlen)

	// Delete it from metadata
	delete(t.hkeys, hkey)

	t.garbage += garbage
	t.inuse -= garbage
	return false
}

func (t *table) updateTTL(hkey uint64, value *VData) bool {
	offset, ok := t.hkeys[hkey]
	if !ok {
		// Try the previous table.
		return true
	}

	// key, 1 byte for key size, klen for key's actual length.
	klen := int(uint8(t.memory[offset]))
	offset += 1 + klen

	// Set the new TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.TTL))
	offset += 8

	// Set the new Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.Timestamp))
	return false
}
