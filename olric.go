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

/*Package olric provides distributed, in-memory and embeddable key/value store, used as a database and cache.*/
package olric

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/flog"
	"github.com/buraksezer/olric/internal/locker"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/serializer"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
)

var (
	// ErrKeyNotFound is returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")

	// ErrInternalServerError means that something unintentionally went wrong while processing the request.
	ErrInternalServerError = errors.New("internal server error")

	// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
	ErrClusterQuorum = errors.New("cannot be reached cluster quorum to operate")

	// ErrUnknownOperation means that an unidentified message has been received from a client.
	ErrUnknownOperation = errors.New("unknown operation")

	ErrServerGone = errors.New("server is gone")

	ErrInvalidArgument = errors.New("invalid argument")

	ErrKeyTooLarge = errors.New("key too large")

	ErrNotImplemented = errors.New("not implemented")
)

// ReleaseVersion is the current stable version of Olric
const ReleaseVersion string = "0.3.0"

const (
	nilTimeout                = 0 * time.Second
	requiredCheckpoints int32 = 2
)

// A full list of alive members. It's required for Pub/Sub and event dispatching systems.
type members struct {
	mtx sync.RWMutex
	m   map[uint64]discovery.Member
}

// Olric implements a distributed, in-memory and embeddable key/value store and cache.
type Olric struct {
	// name is BindAddr:BindPort. It defines servers unique name in the cluster.
	name string

	// These values is useful to control operation status.
	bootstrapped int32
	// numMembers is used to check cluster quorum.
	numMembers int32

	// Number of successfully passed checkpoints
	passedCheckpoints int32

	// Currently owned partition count. Approximate LRU implementation
	// uses that.
	ownedPartitionCount uint64

	// this defines this Olric node in the cluster.
	this   discovery.Member
	config *config.Config
	log    *flog.Logger

	// hasher may be defined by the user. The default one is xxhash
	hasher hasher.Hasher

	// Fine-grained lock implementation. Useful to implement atomic operations
	// and distributed, optimistic lock implementation.
	locker     *locker.Locker
	serializer serializer.Serializer
	discovery  *discovery.Discovery

	// consistent hash ring implementation.
	consistent *consistent.Consistent

	// Logical units for data storage
	partitions map[uint64]*partition
	backups    map[uint64]*partition

	// Matches opcodes to functions. It's somewhat like an HTTP request multiplexer
	operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)

	// Internal TCP server and its client for peer-to-peer communication.
	client *transport.Client
	server *transport.Server

	// A full list of alive members. It's required for Pub/Sub and event dispatching systems.
	// 包含所有活跃节点
	members members

	// Dispatch topic messages
	dtopic *dtopic

	// Bidirectional stream sockets for Olric clients and nodes.
	streams *streams

	// Structures for flow control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	started func()
}

// pool is good for recycling memory while reading messages from the socket.
var bufferPool = bufpool.New()

// New creates a new Olric instance, otherwise returns an error.
func New(c *config.Config) (*Olric, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// 填充默认配置
	err := c.Sanitize()
	if err != nil {
		return nil, err
	}

	// 验证配置合法性
	err = c.Validate()
	if err != nil {
		return nil, err
	}

	// 配置监听地址：c.BindAddr、c.MemberlistConfig.BindAddr、c.MemberlistConfig.AdvertiseAddr
	err = c.SetupNetworkConfig()
	if err != nil {
		return nil, err
	}

	// 覆盖 ml 节点名
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))

	// 构造一致性哈希配置
	cfg := consistent.Config{
		Hasher:            c.Hasher,              // 哈希函数
		PartitionCount:    int(c.PartitionCount), // 分区数
		ReplicationFactor: 20,                    // member 在一致性哈希环上被复制的次数 // TODO: This also may be a configuration param.
		Load:              c.LoadFactor,          // 计算平均负载
	}

	// 初始化 logger
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"}, // 日志级别
		MinLevel: logutils.LogLevel(strings.ToUpper(c.LogLevel)),        // 最小级别
		Writer:   c.LogOutput,                                           // output
	}
	c.Logger.SetOutput(filter)

	flogger := flog.New(c.Logger)
	flogger.SetLevel(c.LogVerbosity)
	if c.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}

	// 构造 rpc client ，内置 addr 维度的 conn pool
	cc := &transport.ClientConfig{
		DialTimeout: c.DialTimeout,
		KeepAlive:   c.KeepAlivePeriod,
		MaxConn:     1024, // TODO: Make this configurable.
	}
	client := transport.NewClient(cc)

	ctx, cancel := context.WithCancel(context.Background())
	db := &Olric{
		name:       c.MemberlistConfig.Name, // 节点名
		ctx:        ctx,
		cancel:     cancel,
		log:        flogger, // logger
		config:     c,       // 配置
		hasher:     c.Hasher,
		locker:     locker.New(),
		serializer: c.Serializer,
		consistent: consistent.New(nil, cfg),
		client:     client,
		partitions: make(map[uint64]*partition),
		backups:    make(map[uint64]*partition),
		operations: make(map[protocol.OpCode]func(w, r protocol.EncodeDecoder)),
		server:     transport.NewServer(c.BindAddr, c.BindPort, c.KeepAlivePeriod, flogger),
		members:    members{m: make(map[uint64]discovery.Member)},
		dtopic:     newDTopic(ctx),
		streams:    &streams{m: make(map[uint64]*stream)},
		started:    c.Started,
	}

	// 用于处理传入请求并返回响应
	db.server.SetDispatcher(db.requestDispatcher)

	// Create all the partitions. It's read-only. No need for locking.
	// 创建 n 个 main partition
	for i := uint64(0); i < c.PartitionCount; i++ {
		db.partitions[i] = &partition{id: i}
	}

	// Create all the backup partitions. It's read-only. No need for locking.
	// 创建 n 个 backup partition
	for i := uint64(0); i < c.PartitionCount; i++ {
		db.backups[i] = &partition{
			id:     i,
			backup: true,
		}
	}

	// 注册请求处理函数
	db.registerOperations()
	return db, nil
}

func (db *Olric) passCheckpoint() {
	atomic.AddInt32(&db.passedCheckpoints, 1)
}

// requestDispatcher 函数用于处理传入的请求
func (db *Olric) requestDispatcher(w, r protocol.EncodeDecoder) {
	// Check bootstrapping status
	// Exclude protocol.OpUpdateRouting. The node is bootstrapped by this operation.
	//
	// 检查节点引导状态：
	//	除了 protocol.OpUpdateRouting 操作外，其他操作在执行前都会检查节点是否完成引导（初始化）状态。
	//  OpUpdateRouting 是一个特殊操作，不需要检查节点的引导状态，它用于更新节点的路由信息。
	if r.OpCode() != protocol.OpUpdateRouting {
		if err := db.checkOperationStatus(); err != nil {
			db.errorResponse(w, err)
			return
		}
	}

	// 根据请求的操作码 r.OpCode()，在 db.operations 中查找对应的处理函数。
	f, ok := db.operations[r.OpCode()]
	if !ok {
		db.errorResponse(w, ErrUnknownOperation)
		return
	}

	// Run the incoming command.
	f(w, r)
}

// bootstrapCoordinator prepares the very first routing table and bootstraps the coordinator node.
func (db *Olric) bootstrapCoordinator() error {
	routingMtx.Lock()
	defer routingMtx.Unlock()

	table := db.distributePartitions()
	_, err := db.updateRoutingTableOnCluster(table)
	if err == nil {
		// The coordinator bootstraps itself.
		atomic.StoreInt32(&db.bootstrapped, 1)
		db.log.V(2).Printf("[INFO] The cluster coordinator has been bootstrapped")
	}
	return err
}

// startDiscovery initializes and starts discovery subsystem.
//
// 创建 discovery 对象并启动；
// 尝试将当前节点加入到 ml 集群中，若失败则重试(最多 N 次)；
// 完成 join 之后(可能失败)，从 discovery 中获取当前节点，若无法获取则报错返回；
// 获取当前 ml 集群成员的数量存储到 db.numMembers 中，以加速获取；
// [重要]启动协程，从 d.ClusterEvents 管道中监听 ml 事件，更新本地路由信息；
// 节点启动时，无法短时发现集群中所有成员，集群中节点数可能不满足 Quorum ，这时不能做任何操作，所以要阻塞等待；
// 集群中节点数达到要求 Quorum，可以继续执行：
//   - 把自己加入到活跃成员列表
//   - 把自己加入到一致性哈希环
//   - 如果当前节点是 coordinator ，需要执行 bootstrap 操作
//
// 启动完毕，信息打印
func (db *Olric) startDiscovery() error {
	d, err := discovery.New(db.log, db.config)
	if err != nil {
		return err
	}
	err = d.Start()
	if err != nil {
		return err
	}
	db.discovery = d

	attempts := 0
	for attempts < db.config.MaxJoinAttempts {
		if !db.isAlive() {
			return nil
		}

		attempts++
		n, err := db.discovery.Join()
		if err == nil {
			db.log.V(2).Printf("[INFO] Join completed. Synced with %d initial nodes", n)
			break
		}

		db.log.V(2).Printf("[ERROR] Join attempt returned error: %s", err)
		if atomic.LoadInt32(&db.bootstrapped) == 1 {
			db.log.V(2).Printf("[INFO] Bootstrapped by the cluster coordinator")
			break
		}

		db.log.V(2).Printf("[INFO] Awaits for %s to join again (%d/%d)", db.config.JoinRetryInterval, attempts, db.config.MaxJoinAttempts)
		<-time.After(db.config.JoinRetryInterval)
	}

	this, err := db.discovery.FindMemberByName(db.name)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to get this node in cluster: %v", err)
		serr := db.discovery.Shutdown()
		if serr != nil {
			return serr
		}
		return err
	}
	db.this = this

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	db.storeNumMembers()

	db.wg.Add(1)
	go db.listenMemberlistEvents(d.ClusterEvents)

	// Check member count quorum now. If there is no enough peers to work, wait forever.
	for {
		// 检查当前集群中的成员数量是否满足配置要求的法定节点数（Quorum）。
		err := db.checkMemberCountQuorum()
		if err == nil {
			// It's OK. Continue as usual.
			break
		}
		// 未达到要求，等待 1s 后重试
		db.log.V(2).Printf("[ERROR] Inoperable node: %v", err)
		select {
		// TODO: Consider making this parametric
		case <-time.After(time.Second):
		case <-db.ctx.Done():
			// the server is gone
			return nil
		}
	}

	// 至此，集群中节点数达到要求 Quorum，可以继续执行

	db.members.mtx.Lock()
	db.members.m[db.this.ID] = db.this
	db.members.mtx.Unlock()
	db.consistent.Add(db.this)
	if db.discovery.IsCoordinator() {
		err = db.bootstrapCoordinator()
		if err == consistent.ErrInsufficientMemberCount {
			db.log.V(2).Printf("[ERROR] Failed to bootstrap the coordinator node: %v", err)
			// Olric will try to form a cluster again.
			err = nil
		}
		if err != nil {
			return err
		}
	}

	if db.config.Interface != "" {
		db.log.V(2).Printf("[INFO] Olric uses interface: %s", db.config.Interface)
	}
	db.log.V(2).Printf("[INFO] Olric bindAddr: %s, bindPort: %d", db.config.BindAddr, db.config.BindPort)
	if db.config.MemberlistInterface != "" {
		db.log.V(2).Printf("[INFO] Memberlist uses interface: %s", db.config.MemberlistInterface)
	}
	db.log.V(2).Printf("[INFO] Memberlist bindAddr: %s, bindPort: %d", db.config.MemberlistConfig.BindAddr, db.config.MemberlistConfig.BindPort)
	db.log.V(2).Printf("[INFO] Cluster coordinator: %s", db.discovery.GetCoordinator())
	return nil
}

// callStartedCallback checks passed checkpoint count and calls the callback function.
func (db *Olric) callStartedCallback() {
	defer db.wg.Done()
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			// 当通过两个检查点之后，才能执行 started 回调
			if requiredCheckpoints == atomic.LoadInt32(&db.passedCheckpoints) {
				if db.started != nil {
					db.started()
				}
				return
			}
		case <-db.ctx.Done():
			return
		}
	}
}

func (db *Olric) errorResponse(w protocol.EncodeDecoder, err error) {
	getError := func(err interface{}) []byte {
		switch val := err.(type) {
		case string:
			return []byte(val)
		case error:
			return []byte(val.Error())
		default:
			return nil
		}
	}
	w.SetValue(getError(err))

	switch {
	case err == ErrWriteQuorum, errors.Is(err, ErrWriteQuorum):
		w.SetStatus(protocol.StatusErrWriteQuorum)
	case err == ErrReadQuorum, errors.Is(err, ErrReadQuorum):
		w.SetStatus(protocol.StatusErrReadQuorum)
	case err == ErrNoSuchLock, errors.Is(err, ErrNoSuchLock):
		w.SetStatus(protocol.StatusErrNoSuchLock)
	case err == ErrLockNotAcquired, errors.Is(err, ErrLockNotAcquired):
		w.SetStatus(protocol.StatusErrLockNotAcquired)
	case err == ErrKeyNotFound, err == storage.ErrKeyNotFound:
		w.SetStatus(protocol.StatusErrKeyNotFound)
	case errors.Is(err, ErrKeyNotFound), errors.Is(err, storage.ErrKeyNotFound):
		w.SetStatus(protocol.StatusErrKeyNotFound)
	case err == ErrKeyTooLarge, err == storage.ErrKeyTooLarge:
		w.SetStatus(protocol.StatusErrKeyTooLarge)
	case errors.Is(err, ErrKeyTooLarge), errors.Is(err, storage.ErrKeyTooLarge):
		w.SetStatus(protocol.StatusErrKeyTooLarge)
	case err == ErrOperationTimeout, errors.Is(err, ErrOperationTimeout):
		w.SetStatus(protocol.StatusErrOperationTimeout)
	case err == ErrKeyFound, errors.Is(err, ErrKeyFound):
		w.SetStatus(protocol.StatusErrKeyFound)
	case err == ErrClusterQuorum, errors.Is(err, ErrClusterQuorum):
		w.SetStatus(protocol.StatusErrClusterQuorum)
	case err == ErrUnknownOperation, errors.Is(err, ErrUnknownOperation):
		w.SetStatus(protocol.StatusErrUnknownOperation)
	case err == ErrEndOfQuery, errors.Is(err, ErrEndOfQuery):
		w.SetStatus(protocol.StatusErrEndOfQuery)
	case err == ErrServerGone, errors.Is(err, ErrServerGone):
		w.SetStatus(protocol.StatusErrServerGone)
	case err == ErrInvalidArgument, errors.Is(err, ErrInvalidArgument):
		w.SetStatus(protocol.StatusErrInvalidArgument)
	case err == ErrNotImplemented, errors.Is(err, ErrNotImplemented):
		w.SetStatus(protocol.StatusErrNotImplemented)
	default:
		w.SetStatus(protocol.StatusInternalServerError)
	}
}

func (db *Olric) requestTo(addr string, req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	resp, err := db.client.RequestTo(addr, req)
	if err != nil {
		return nil, err
	}
	status := resp.Status()
	switch {
	case status == protocol.StatusOK:
		return resp, nil
	case status == protocol.StatusInternalServerError:
		return nil, errors.Wrap(ErrInternalServerError, string(resp.Value()))
	case status == protocol.StatusErrNoSuchLock:
		return nil, ErrNoSuchLock
	case status == protocol.StatusErrLockNotAcquired:
		return nil, ErrLockNotAcquired
	case status == protocol.StatusErrKeyNotFound:
		return nil, ErrKeyNotFound
	case status == protocol.StatusErrWriteQuorum:
		return nil, ErrWriteQuorum
	case status == protocol.StatusErrReadQuorum:
		return nil, ErrReadQuorum
	case status == protocol.StatusErrOperationTimeout:
		return nil, ErrOperationTimeout
	case status == protocol.StatusErrKeyFound:
		return nil, ErrKeyFound
	case status == protocol.StatusErrClusterQuorum:
		return nil, ErrClusterQuorum
	case status == protocol.StatusErrEndOfQuery:
		return nil, ErrEndOfQuery
	case status == protocol.StatusErrUnknownOperation:
		return nil, ErrUnknownOperation
	case status == protocol.StatusErrServerGone:
		return nil, ErrServerGone
	case status == protocol.StatusErrInvalidArgument:
		return nil, ErrInvalidArgument
	case status == protocol.StatusErrKeyTooLarge:
		return nil, ErrKeyTooLarge
	case status == protocol.StatusErrNotImplemented:
		return nil, ErrNotImplemented
	}
	return nil, fmt.Errorf("unknown status code: %d", status)
}

func (db *Olric) isAlive() bool {
	select {
	case <-db.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

// checkBootstrap is called for every request and checks whether the node is bootstrapped.
// It has to be very fast for a smooth operation.
//
// 检查当前节点是否已经完成引导，若没完成则进入循环等待，每 100 毫秒检查一次，直到完成引导或者节点退出。
func (db *Olric) checkBootstrap() error {
	// check it immediately
	if atomic.LoadInt32(&db.bootstrapped) == 1 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.config.RequestTimeout)
	defer cancel()

	// This loop only works for the first moments of the process.
	for {
		if atomic.LoadInt32(&db.bootstrapped) == 1 {
			return nil
		}
		<-time.After(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ErrOperationTimeout
		default:
		}
	}
}

// storeNumMembers assigns the current number of members in the cluster to a variable.
//
// 获取当前集群成员的数量存储到 db.numMembers 中，因为成员数量的变化不频繁，这样能避免频繁调用 NumMembers() 接口。
func (db *Olric) storeNumMembers() {
	// Calling NumMembers in every request is quite expensive.
	// It's rarely updated. Just call this when the membership info changed.
	nr := int32(db.discovery.NumMembers())
	atomic.StoreInt32(&db.numMembers, nr)
}

// 检查当前集群中的成员数量是否满足配置要求的法定节点数（Quorum）。
// 如果当前成员数量低于所需的法定人数，函数将返回一个错误 ErrClusterQuorum 。
func (db *Olric) checkMemberCountQuorum() error {
	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	nr := atomic.LoadInt32(&db.numMembers)
	if db.config.MemberCountQuorum > nr {
		return ErrClusterQuorum
	}
	return nil
}

// checkOperationStatus controls bootstrapping status and cluster quorum to prevent split-brain syndrome.
// 检查当前集群中的成员数量是否满足配置要求的法定节点数（Quorum） && 检查当前节点是否已经完成引导；
func (db *Olric) checkOperationStatus() error {
	if err := db.checkMemberCountQuorum(); err != nil {
		return err
	}
	// An Olric node has to be bootstrapped to function properly.
	return db.checkBootstrap()
}

// Start starts background servers and joins the cluster. You still need to call Shutdown method if
// Start function returns an early error.
func (db *Olric) Start() error {

	// 启动 ListenAndServe 协程：负责接收连接、处理请求、写回响应
	errCh := make(chan error, 1)
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		errCh <- db.server.ListenAndServe()
	}()

	// 等待 ListenAndServe 协程启动完毕或失败
	<-db.server.StartCh
	select {
	case err := <-errCh:
		return err
	default:
	}

	// TCP server is started
	// 检查点 1
	db.passCheckpoint()

	// 启动服务发现，用于初始化 ml 集群、维护路由表、处理成员变更事件；
	if err := db.startDiscovery(); err != nil {
		return err
	}

	// Memberlist is started and this node joined the cluster.
	// 检查点 2
	db.passCheckpoint()

	// Warn the user about its choice of configuration
	if db.config.ReplicationMode == config.AsyncReplicationMode && db.config.WriteQuorum > 1 {
		db.log.V(2).Printf("[WARN] Olric is running in async replication mode. WriteQuorum (%d) is ineffective", db.config.WriteQuorum)
	}
	db.log.V(2).Printf("[INFO] Node name in the cluster: %s", db.name)

	// Start periodic tasks.
	db.wg.Add(2)
	go db.updateRoutingPeriodically() // 如果是 coordinator ，每分钟更新一次路由表并广播到 ml 集群
	go db.evictKeysAtBackground()     //

	// 启动成功回调
	if db.started != nil {
		db.wg.Add(1)
		go db.callStartedCallback()
	}

	return <-errCh
}

// Shutdown stops background servers and leaves the cluster.
func (db *Olric) Shutdown(ctx context.Context) error {
	db.cancel()

	var result error

	db.streams.mu.RLock()
	db.log.V(2).Printf("[INFO] Closing active streams")
	for _, s := range db.streams.m {
		s.close()
	}
	db.streams.mu.RUnlock()

	if err := db.server.Shutdown(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	if db.discovery != nil {
		err := db.discovery.Shutdown()
		if err != nil {
			result = multierror.Append(result, err)
		}
	}

	db.wg.Wait()

	// If the user kills the server before bootstrapping, db.this is going to empty.
	db.log.V(2).Printf("[INFO] %s is gone", db.name)
	return result
}

func getTTL(timeout time.Duration) int64 {
	// convert nanoseconds to milliseconds
	return (timeout.Nanoseconds() + time.Now().UnixNano()) / 1000000
}

func isKeyExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}
	// convert nanoseconds to milliseconds
	return (time.Now().UnixNano() / 1000000) >= ttl
}

// hostCmp returns true if o1 and o2 is the same.
func hostCmp(o1, o2 discovery.Member) bool {
	return o1.ID == o2.ID
}
