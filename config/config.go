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

package config

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/serializer"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/memberlist"
)

const (
	// SyncReplicationMode enables sync replication mode which means that the caller is blocked
	// until write/delete operation is applied by replica owners. The default mode is SyncReplicationMode
	SyncReplicationMode = 0

	// AsyncReplicationMode enables async replication mode which means that write/delete operations
	// are done in a background task.
	AsyncReplicationMode = 1
)

const (
	// DefaultPort is for Olric
	DefaultPort = 3320

	// DefaultDiscoveryPort is for memberlist
	DefaultDiscoveryPort = 3322

	// DefaultPartitionCount denotes default partition count in the cluster.
	DefaultPartitionCount = 271

	// DefaultLoadFactor is used by the consistent hashing function. Keep it small.
	DefaultLoadFactor = 1.25

	// DefaultLogLevel determines the log level without extra configuration. It's DEBUG.
	DefaultLogLevel = "DEBUG"

	// DefaultLogVerbosity denotes default log verbosity level.
	//
	// * flog.V(1) - Generally useful for this to ALWAYS be visible to an operator
	//   * Programmer errors
	//   * Logging extra info about a panic
	//   * CLI argument handling
	// * flog.V(2) - A reasonable default log level if you don't want verbosity.
	//   * Information about config (listening on X, watching Y)
	//   * Errors that repeat frequently that relate to conditions that can be corrected (pod detected as unhealthy)
	// * flog.V(3) - Useful steady state information about the service and important log messages that may correlate to
	//   significant changes in the system.  This is the recommended default log level for most systems.
	//   * Logging HTTP requests and their exit code
	//   * System state changing (killing pod)
	//   * Controller state change events (starting pods)
	//   * Scheduler log messages
	// * flog.V(4) - Extended information about changes
	//   * More info about system state changes
	// * flog.V(5) - Debug level verbosity
	//   * Logging in particularly thorny parts of code where you may want to come back later and check it
	// * flog.V(6) - Trace level verbosity
	//   * Context to understand the steps leading up to errors and warnings
	//   * More information for troubleshooting reported issues
	DefaultLogVerbosity = 3

	// MinimumReplicaCount denotes default and minimum replica count in an Olric cluster.
	MinimumReplicaCount = 1

	// DefaultRequestTimeout denotes default timeout value for a request.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultJoinRetryInterval denotes a time gap between sequential join attempts.
	DefaultJoinRetryInterval = time.Second

	// DefaultMaxJoinAttempts denotes a maximum number of failed join attempts
	// before forming a standalone cluster.
	DefaultMaxJoinAttempts = 10

	// MinimumMemberCountQuorum denotes minimum required count of members to form a cluster.
	MinimumMemberCountQuorum = 1

	// DefaultTableSize is 1MB if you don't set your own value.
	DefaultTableSize = 1 << 20

	DefaultLRUSamples int = 5

	// Assign this as EvictionPolicy in order to enable LRU eviction algorithm.
	LRUEviction EvictionPolicy = "LRU"
)

// EvictionPolicy denotes eviction policy. Currently: LRU or NONE.
type EvictionPolicy string

// note on DMapCacheConfig and CacheConfig:
// golang doesn't provide the typical notion of inheritance.
// because of that I preferred to define the types explicitly.

// DMapCacheConfig denotes cache configuration for a particular dmap.
//
// 为特定的 DMap 配置缓存策略，以实现更精细的缓存管理。
type DMapCacheConfig struct {
	// MaxIdleDuration denotes maximum time for each entry to stay idle in the dmap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration
	// feature varies by preferred deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a dmap instance.
	TTLDuration time.Duration

	// MaxKeys denotes maximum key count on a particular node. So if you have 10 nodes with
	// MaxKeys=100000, your key count in the cluster should be around MaxKeys*10=1000000
	MaxKeys int

	// MaxInuse denotes maximum amount of in-use memory on a particular node. So if you have 10 nodes with
	// MaxInuse=100M (it has to be in bytes), amount of in-use memory should be around MaxInuse*10=1G
	MaxInuse int

	// LRUSamples denotes amount of randomly selected key count by the aproximate LRU implementation.
	// Lower values are better for high performance. It's 5 by default.
	LRUSamples int

	// EvictionPolicy determines the eviction policy in use. It's NONE by default.
	// Set as LRU to enable LRU eviction policy.
	EvictionPolicy EvictionPolicy
}

// CacheConfig denotes a global cache configuration for DMaps. You can still overwrite it by setting a
// DMapCacheConfig for a particular dmap. Don't set this if you use Olric as an ordinary key/value store.
type CacheConfig struct {
	// NumEvictionWorkers denotes the number of goroutines that's used to find keys for eviction.
	// 用于执行键淘汰（Eviction）操作的 Goroutine 数量。
	NumEvictionWorkers int64

	// MaxIdleDuration denotes maximum time for each entry to stay idle in the dmap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration
	// feature varies by preferred deployment method.
	//
	// 如果某个键在一段时间内没有任何操作（如 Get, Put, PutEx, Expire, PutIf, PutIfEx 等），它会被标记为空闲并自动被移除。
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a dmap instance.
	// 为每个键设置默认的 TTL（生存时间），TTL 到期后该键会被自动移除。
	TTLDuration time.Duration

	// MaxKeys denotes maximum key count on a particular node. So if you have 10 nodes with
	// MaxKeys=100000, max key count in the cluster should around MaxKeys*10=1000000
	//
	// 设置每个节点上允许存储的最大键数量。
	// 如果集群中有 10 个节点，且每个节点的 MaxKeys 设置为 100000，则整个集群的最大键数量大约为 MaxKeys * 10 = 1000000。
	MaxKeys int

	// MaxInuse denotes maximum amount of in-use memory on a particular node. So if you have 10 nodes with
	// MaxInuse=100M (it has to be in bytes), max amount of in-use memory should be around MaxInuse*10=1G
	//
	// 指定每个节点上使用内存的最大字节数。
	// 如果集群中有 10 个节点，每个节点的 MaxInuse 设置为 100M，则集群总共可以使用的最大内存大约为 MaxInuse * 10 = 1G。
	// 一旦达到内存上限，节点将自动开始淘汰部分数据，以释放内存。
	MaxInuse int

	// LRUSamples denotes amount of randomly selected key count by the aproximate LRU implementation.
	// Lower values are better for high performance. It's 5 by default.
	//
	// 指定 LRU 实现中每次随机选择的键数量。
	// 使用近似 LRU 实现（基于随机采样）来选择要淘汰的键，较低的采样数值能提升性能。默认值为 5 。
	LRUSamples int

	// EvictionPolicy determines the eviction policy in use. It's NONE by default.
	// Set as LRU to enable LRU eviction policy.
	//
	// 设置使用的淘汰策略。
	// 默认是 NONE，表示没有启用任何淘汰策略。
	// 可以设置为 LRU，启用基于最近最少使用（Least Recently Used）的淘汰策略。
	EvictionPolicy EvictionPolicy

	// DMapConfigs is useful to set custom cache config per dmap instance.
	//
	// 用于为特定的 DMap 实例配置自定义缓存设置，例如某些 DMap 可能需要不同的 TTL 或淘汰策略等。
	DMapConfigs map[string]DMapCacheConfig
}

// Config is the configuration to create a Olric instance.
type Config struct {
	// Interface denotes a binding interface. It can be used instead of BindAddr if the interface is known but not the address.
	// If both are provided, then Olric verifies that the interface has the bind address that is provided.
	//
	// 绑定的网络接口。
	// 如果已知网络接口但不知道地址，可以用它代替 BindAddr，如果两者都提供，Olric 会验证网络接口是否具有 BindAddr 网络地址。
	Interface string

	// LogVerbosity denotes the level of message verbosity. The default value is 3. Valid values are between 1 to 6.
	// 日志消息的详细程度。默认值是 3 ，有效值在 1 到 6 之间。
	LogVerbosity int32

	// Default LogLevel is DEBUG. Valid ones: "DEBUG", "WARN", "ERROR", "INFO"
	// 日志级别。有效值："DEBUG", "WARN", "ERROR", "INFO"
	LogLevel string

	// BindAddr denotes the address that Olric will bind to for communication with other Olric nodes.
	// 绑定地址，用于与其他 Olric 节点通信
	BindAddr string

	// BindPort denotes the address that Olric will bind to for communication with other Olric nodes.
	// 绑定端口，用于与其他 Olric 节点通信
	BindPort int

	// KeepAlivePeriod denotes whether the operating system should send keep-alive messages on the connection.
	// 操作系统是否应在连接上发送保活消息。
	KeepAlivePeriod time.Duration

	// Timeout for TCP dial.
	//
	// The timeout includes name resolution, if required. When using TCP, and the host in the address parameter
	// resolves to multiple IP addresses, the timeout is spread over each consecutive dial, such that each is
	// given an appropriate fraction of the time to connect.
	//
	// TCP 拨号的超时时间。
	DialTimeout time.Duration

	RequestTimeout time.Duration

	// The list of host:port which are used by memberlist for discovery. Don't confuse it with Name.
	// memberlist 种子节点
	Peers []string

	// PartitionCount is 271, by default.
	// 分区数
	PartitionCount uint64

	// ReplicaCount is 1, by default.
	// 副本数
	ReplicaCount int

	// Minimum number of successful reads to return a response for a read request.
	// 读请求所需的最小成功节点数
	ReadQuorum int

	// Minimum number of successful writes to return a response for a write request.
	// 写请求所需的最小成功节点数
	WriteQuorum int

	// Minimum number of members to form a cluster and run any query on the cluster.
	// 形成集群并在集群上运行任何操作所需的最小成员数。
	MemberCountQuorum int32

	// Switch to control read-repair algorithm which helps to reduce entropy.
	// 控制读修复算法的开关，该算法有助于减少熵。
	ReadRepair bool

	// Default value is SyncReplicationMode.
	ReplicationMode int

	// LoadFactor is used by consistent hashing function. It determines the maximum load
	// for a server in the cluster. Keep it small.
	// LoadFactor 由一致性哈希函数使用。
	LoadFactor float64

	// Default hasher is github.com/cespare/xxhash
	Hasher hasher.Hasher

	// Default Serializer implementation uses gob for encoding/decoding.
	// 默认使用 gob 进行编码/解码。
	Serializer serializer.Serializer

	// LogOutput is the writer where logs should be sent. If this is not
	// set, logging will go to stderr by default. You cannot specify both LogOutput
	// and Logger at the same time.
	//
	// LogOutput 指定日志写入到哪里。
	// 如果未设置，日志将默认发送到 stderr。
	// 不能同时指定 LogOutput 和 Logger。
	LogOutput io.Writer

	// Logger is a custom logger which you provide. If Logger is set, it will use
	// this for the internal logger. If Logger is not set, it will fall back to the
	// behavior for using LogOutput. You cannot specify both LogOutput and Logger
	// at the same time.
	//
	// Logger 是您提供的自定义日志记录器。如果设置了 Logger，将使用此内部日志记录器。
	// 如果未设置 Logger，将回退到使用 LogOutput 的行为。
	// 不能同时指定 LogOutput 和 Logger。
	Logger *log.Logger

	//
	Cache *CacheConfig

	// Minimum size(in-bytes) for append-only file
	// 文件大小（以字节为单位）
	TableSize int

	JoinRetryInterval time.Duration
	MaxJoinAttempts   int

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	//
	// 回调函数。Olric 在服务器准备好接受新连接后调用此函数。
	Started func()

	ServiceDiscovery map[string]interface{}

	// Interface denotes a binding interface. It can be used instead of memberlist.Config.BindAddr if the interface is
	// known but not the address. If both are provided, then Olric verifies that the interface has the bind address that
	// is provided.
	//
	// MemberlistInterface 表示 Memberlist 绑定的网络接口。
	// 如果已知接口但不知道地址，可以用 MemberlistInterface 代替 memberlist.Config.BindAddr 使用，如果两者都提供，Olric 会验证接口是否具有提供的绑定地址。
	MemberlistInterface string

	// MemberlistConfig is the memberlist configuration that Olric will
	// use to do the underlying membership management and gossip. Some
	// fields in the MemberlistConfig will be overwritten by Olric no
	// matter what:
	//
	//   * Name - This will always be set to the same as the NodeName
	//     in this configuration.
	//
	//   * ClusterEvents - Olric uses a custom event delegate.
	//
	//   * Delegate - Olric uses a custom delegate.
	//
	// You have to use NewMemberlistConfig to create a new one.
	// Then, you may need to modify it to tune for your environment.
	//
	// MemberlistConfig 是 Olric 将用于执行成员管理的 memberlist 配置。
	// 无论如何，Olric 都会覆盖 MemberlistConfig 中的一些字段：
	//   * Name - 这将始终设置为与此配置中的 NodeName 相同。
	//   * ClusterEvents - Olric 使用自定义事件委托。
	//   * Delegate - Olric 使用自定义委托。
	MemberlistConfig *memberlist.Config
}

// NewMemberlistConfig returns a new memberlist.Config from vendored version of that package.
// It takes an env parameter: local, lan and wan.
//
// local:
// DefaultLocalConfig works like DefaultConfig, however it returns a configuration that
// is optimized for a local loopback environments. The default configuration is still very conservative
// and errs on the side of caution.
//
// lan:
// DefaultLANConfig returns a sane set of configurations for Memberlist. It uses the hostname
// as the node name, and otherwise sets very conservative values that are sane for most LAN environments.
// The default configuration errs on the side of caution, choosing values that are optimized for higher convergence
// at the cost of higher bandwidth usage. Regardless, these values are a good starting point when getting started with memberlist.
//
// wan:
// DefaultWANConfig works like DefaultConfig, however it returns a configuration that is optimized for most WAN environments.
// The default configuration is still very conservative and errs on the side of caution.
func NewMemberlistConfig(env string) (*memberlist.Config, error) {
	e := strings.ToLower(env)
	switch e {
	case "local":
		return memberlist.DefaultLocalConfig(), nil
	case "lan":
		return memberlist.DefaultLANConfig(), nil
	case "wan":
		return memberlist.DefaultWANConfig(), nil
	}
	return nil, fmt.Errorf("unknown env: %s", env)
}

func (c *Config) validateMemberlistConfig() error {
	var result error
	if len(c.MemberlistConfig.AdvertiseAddr) != 0 {
		if ip := net.ParseIP(c.MemberlistConfig.AdvertiseAddr); ip == nil {
			result = multierror.Append(result,
				fmt.Errorf("memberlist: AdvertiseAddr has to be a valid IPv4 or IPv6 address"))
		}
	}
	if len(c.MemberlistConfig.BindAddr) == 0 {
		result = multierror.Append(result,
			fmt.Errorf("memberlist: BindAddr cannot be an empty string"))
	}
	return result
}

// Validate validates the given configuration.
func (c *Config) Validate() error {
	var result error

	// 副本数至少为 1
	if c.ReplicaCount < MinimumReplicaCount {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify ReplicaCount smaller than MinimumReplicaCount"))
	}

	if c.ReadQuorum <= 0 {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify ReadQuorum less than or equal to zero"))
	}
	if c.ReplicaCount < c.ReadQuorum {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify ReadQuorum greater than ReplicaCount"))
	}

	if c.WriteQuorum <= 0 {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify WriteQuorum less than or equal to zero"))
	}
	if c.ReplicaCount < c.WriteQuorum {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify WriteQuorum greater than ReplicaCount"))
	}

	if err := c.validateMemberlistConfig(); err != nil {
		result = multierror.Append(result, err)
	}

	if c.MemberCountQuorum < MinimumMemberCountQuorum {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify MemberCountQuorum "+
				"smaller than MinimumMemberCountQuorum"))
	}

	if c.BindAddr == "" {
		result = multierror.Append(result, fmt.Errorf("BindAddr cannot be empty"))
	}

	if c.BindPort == 0 {
		result = multierror.Append(result, fmt.Errorf("BindPort cannot be empty or zero"))
	}

	return result
}

// Sanitize sanitizes the given configuration.
// It returns an error if there is something very bad in the configuration.
func (c *Config) Sanitize() error {
	if c.Logger == nil {
		if c.LogOutput == nil {
			c.LogOutput = os.Stderr
		}
		if c.LogLevel == "" {
			c.LogLevel = DefaultLogLevel
		}
		c.Logger = log.New(c.LogOutput, "", log.LstdFlags)
	}

	if c.LogVerbosity <= 0 {
		c.LogVerbosity = DefaultLogVerbosity
	}

	if c.Hasher == nil {
		c.Hasher = hasher.NewDefaultHasher()
	}
	if c.Serializer == nil {
		c.Serializer = serializer.NewGobSerializer()
	}

	if c.BindAddr == "" {
		name, err := os.Hostname()
		if err != nil {
			return err
		}
		c.BindAddr = name
	}

	// We currently don't support ephemeral port selection. Because it needs improved flow
	// control in server initialization stage.
	if c.BindPort == 0 {
		c.BindPort = DefaultPort
	}

	if c.LoadFactor == 0 {
		c.LoadFactor = DefaultLoadFactor
	}
	if c.PartitionCount == 0 {
		c.PartitionCount = DefaultPartitionCount
	}
	if c.ReplicaCount == 0 {
		c.ReplicaCount = MinimumReplicaCount
	}
	if c.MemberlistConfig == nil {
		m := memberlist.DefaultLocalConfig()
		// hostname is assigned to memberlist.BindAddr
		// memberlist.Name is assigned by olric.New
		m.BindPort = DefaultDiscoveryPort
		m.AdvertisePort = DefaultDiscoveryPort
		c.MemberlistConfig = m
	}
	if c.RequestTimeout == 0*time.Second {
		c.RequestTimeout = DefaultRequestTimeout
	}
	if c.JoinRetryInterval == 0*time.Second {
		c.JoinRetryInterval = DefaultJoinRetryInterval
	}
	if c.MaxJoinAttempts == 0 {
		c.MaxJoinAttempts = DefaultMaxJoinAttempts
	}
	if c.TableSize == 0 {
		c.TableSize = DefaultTableSize
	}

	// Check peers. If Peers slice contains node's itself, return an error.
	port := strconv.Itoa(c.MemberlistConfig.BindPort)
	this := net.JoinHostPort(c.MemberlistConfig.BindAddr, port)
	for _, peer := range c.Peers {
		if this == peer {
			return fmt.Errorf("a node cannot be peer with itself")
		}
	}
	return nil
}

// DefaultConfig returns a Config with sane defaults.
// It takes an env parameter used by memberlist: local, lan and wan.
//
// local:
//
// DefaultLocalConfig works like DefaultConfig, however it returns a configuration that
// is optimized for a local loopback environments. The default configuration is still very conservative
// and errs on the side of caution.
//
// lan:
//
// DefaultLANConfig returns a sane set of configurations for Memberlist. It uses the hostname
// as the node name, and otherwise sets very conservative values that are sane for most LAN environments.
// The default configuration errs on the side of caution, choosing values that are optimized for higher convergence
// at the cost of higher bandwidth usage. Regardless, these values are a good starting point when getting started with memberlist.
//
// wan:
//
// DefaultWANConfig works like DefaultConfig, however it returns a configuration that is optimized for most WAN environments.
// The default configuration is still very conservative and errs on the side of caution.
func New(env string) *Config {
	c := &Config{
		BindAddr:          "0.0.0.0",
		BindPort:          DefaultPort,
		ReadRepair:        false,
		ReplicaCount:      1,
		WriteQuorum:       1,
		ReadQuorum:        1,
		MemberCountQuorum: 1,
		Peers:             []string{},
		Cache:             &CacheConfig{},
	}
	if err := c.Sanitize(); err != nil {
		panic(fmt.Sprintf("unable to sanitize Olric config: %v", err))
	}
	m, err := NewMemberlistConfig(env)
	if err != nil {
		panic(fmt.Sprintf("unable to create a new memberlist config: %v", err))
	}
	// memberlist.Name will be assigned by olric.New
	m.BindPort = DefaultDiscoveryPort
	m.AdvertisePort = DefaultDiscoveryPort
	c.MemberlistConfig = m

	if err := c.Validate(); err != nil {
		panic(fmt.Sprintf("unable to validate Olric config: %v", err))
	}
	return c
}
