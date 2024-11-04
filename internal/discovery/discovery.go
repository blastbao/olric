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

/*Package discovery provides a basic memberlist integration.*/
package discovery

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"plugin"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/flog"
	"github.com/buraksezer/olric/pkg/service_discovery"
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack"
)

const eventChanCapacity = 256

// ErrHostNotFound indicates that the requested host could not be found in the member list.
var ErrHostNotFound = errors.New("host not found")

// ClusterEvent is a single event related to node activity in the memberlist.
// The Node member of this struct must not be directly modified.
type ClusterEvent struct {
	Event    memberlist.NodeEventType
	NodeName string
	NodeAddr net.IP
	NodePort uint16
	NodeMeta []byte // Metadata from the delegate for this node.
}

func (c *ClusterEvent) MemberAddr() string {
	port := strconv.Itoa(int(c.NodePort))
	return net.JoinHostPort(c.NodeAddr.String(), port)
}

// Discovery is a structure that encapsulates memberlist and
// provides useful functions to utilize it.
type Discovery struct {
	log        *flog.Logger
	host       *Member
	memberlist *memberlist.Memberlist
	config     *config.Config

	// To manage Join/Leave/Update events
	clusterEventsMtx sync.RWMutex
	ClusterEvents    chan *ClusterEvent

	// Try to reconnect dead members
	deadMembers      map[string]int64
	deadMemberEvents chan *ClusterEvent

	eventSubscribers []chan *ClusterEvent
	serviceDiscovery service_discovery.ServiceDiscovery

	// Flow control
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// Member represents a node in the cluster.
type Member struct {
	Name      string
	ID        uint64
	Birthdate int64
}

func (m Member) String() string {
	return m.Name
}

func (d *Discovery) DecodeNodeMeta(buf []byte) (Member, error) {
	res := &Member{}
	err := msgpack.Unmarshal(buf, res)
	return *res, err
}

// New creates a new memberlist with a proper configuration and returns a new Discovery instance along with it.
func New(log *flog.Logger, c *config.Config) (*Discovery, error) {

	// Calculate host's identity. It's useful to compare hosts.
	// 创建时间，纳秒时间戳
	birthdate := time.Now().UnixNano()

	// buf := ts(8B) + name(xxB)
	buf := make([]byte, 8+len(c.MemberlistConfig.Name))
	binary.BigEndian.PutUint64(buf, uint64(birthdate))
	buf = append(buf, []byte(c.MemberlistConfig.Name)...)

	// 基于 buf 计算 hash 值作为节点 ID
	id := c.Hasher.Sum64(buf)
	host := &Member{
		Name:      c.MemberlistConfig.Name,
		ID:        id,
		Birthdate: birthdate,
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := &Discovery{
		host:        host,
		config:      c,
		log:         log,
		deadMembers: make(map[string]int64),
		ctx:         ctx,
		cancel:      cancel,
	}

	// 如果指定了 ServiceDiscovery ，里面可能指定了 plugin 或者 plugin path ，加载进来。
	// ServiceDiscovery 用于发现 ml peers
	if c.ServiceDiscovery != nil {
		if err := d.loadServiceDiscoveryPlugin(); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (d *Discovery) loadServiceDiscoveryPlugin() error {
	var sd service_discovery.ServiceDiscovery

	if val, ok := d.config.ServiceDiscovery["plugin"]; ok {
		if sd, ok = val.(service_discovery.ServiceDiscovery); !ok {
			return fmt.Errorf("plugin type %T is not a ServiceDiscovery interface", val)
		}
	} else {
		pluginPath, ok := d.config.ServiceDiscovery["path"]
		if !ok {
			return fmt.Errorf("plugin path could not be found")
		}
		plug, err := plugin.Open(pluginPath.(string))
		if err != nil {
			return fmt.Errorf("failed to open plugin: %w", err)
		}

		symDiscovery, err := plug.Lookup("ServiceDiscovery")
		if err != nil {
			return fmt.Errorf("failed to lookup serviceDiscovery symbol: %w", err)
		}

		if sd, ok = symDiscovery.(service_discovery.ServiceDiscovery); !ok {
			return fmt.Errorf("unable to assert type to serviceDiscovery")
		}
	}

	if err := sd.SetConfig(d.config.ServiceDiscovery); err != nil {
		return err
	}

	sd.SetLogger(d.config.Logger)
	if err := sd.Initialize(); err != nil {
		return err
	}

	d.serviceDiscovery = sd
	return nil
}

func (d *Discovery) dialDeadMember(member string) {
	// Knock knock
	// TODO: Make this parametric
	// 建立连接，成功则 err == nil
	conn, err := net.DialTimeout("tcp", member, 100*time.Millisecond)
	if err != nil {
		d.log.V(5).Printf("[ERROR] Failed to dial member: %s: %v", member, err)
		return
	}

	// 建连成功，关闭连接
	if err = conn.Close(); err != nil {
		d.log.V(5).Printf("[ERROR] Failed to close connection: %s: %v", member, err)
		// network partitioning continues
		return
	}

	// Everything seems fine. Try to re-join!
	// 尝试将当前节点加入 member 所在集群中
	_, err = d.Rejoin([]string{member})
	if err != nil {
		d.log.V(5).Printf("[ERROR] Failed to re-join: %s: %v", member, err)
	}
}

// 当节点 leave 时，可能是假离线或者很快就恢复，应该给一个重新试探的机会，来确保它确实是离开了。
// 所以，当收到 NodeLeave 消息时，先暂时记录下来，后面定时检查是否真的离线，如果还在线要重新加回来。
//
// 分支一：
//
//	当收到 NodeLeave 事件时，将 member 加入到 deadMembers{} 中；
//	当收到 NodeJoin 事件时，将 member 从 deadMembers{} 中移除；
//
// 分支二：
//
//	每秒钟从 deadMembers{} 中随机取一个 deadMember 来 ping ，如果成功则 rejoin 否则从 deadMembers 中删除；
//
// 备注：
//
// 事件 NodeLeave 通常在集群中的某个节点主动离开或与集群失联时触发。
//
// ## 节点主动退出 ##
// 当集群中的一个节点主动调用 memberlist.Leave() 方法时，该节点会向集群中的其他节点广播它即将离开的消息。
// 其他节点在接收到这个广播消息后，会触发 NodeLeave 事件，表示该节点已主动退出集群。
//
// ## 节点意外失联 ##
// 如果某个节点意外断开连接，例如网络故障、节点崩溃、服务器关机等，集群中的其他节点在一定的超时时间后会认为该节点失联。
// 在失联超时后，memberlist 会触发 NodeLeave 事件来通知其他节点，这个失联的节点已不再是集群的一部分。
func (d *Discovery) deadMemberTracker() {
	d.wg.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case e := <-d.deadMemberEvents:
			member := e.MemberAddr()
			if e.Event == memberlist.NodeJoin {
				delete(d.deadMembers, member)
			} else if e.Event == memberlist.NodeLeave {
				d.deadMembers[member] = time.Now().UnixNano()
			} else {
				d.log.V(2).Printf("[ERROR] Unknown memberlist event received for: %s: %v", e.NodeName, e.Event)
			}
		case <-time.After(time.Second):
			// 每秒钟尝试连接一个 dead member
			// TODO: make this parametric
			// Try to reconnect a random dead member every second.
			// The Go runtime selects a random item in the map
			for member, timestamp := range d.deadMembers {
				// 尝试将当前节点加入 member 所在集群中
				d.dialDeadMember(member)
				// TODO: Make this parametric
				if time.Now().Add(24*time.Hour).UnixNano() >= timestamp {
					delete(d.deadMembers, member)
				}
				break
			}

			// Just try one item
		}
	}
}

func (d *Discovery) Start() error {
	// ClusterEvents chan is consumed by the Olric package to maintain a consistent hash ring.
	d.ClusterEvents = d.SubscribeNodeEvents()    // 订阅 ml 事件
	d.deadMemberEvents = d.SubscribeNodeEvents() // 订阅 ml 事件

	// Initialize a new memberlist
	dl, err := d.newDelegate()
	if err != nil {
		return err
	}
	eventsCh := make(chan memberlist.NodeEvent, eventChanCapacity)
	d.config.MemberlistConfig.Delegate = dl
	d.config.MemberlistConfig.Logger = d.config.Logger
	d.config.MemberlistConfig.Events = &memberlist.ChannelEventDelegate{
		Ch: eventsCh,
	}
	list, err := memberlist.Create(d.config.MemberlistConfig)
	if err != nil {
		return err
	}

	d.memberlist = list

	// 如果配置了 serviceDiscovery 就注册一下，方便别的节点发现自己
	if d.serviceDiscovery != nil {
		if err := d.serviceDiscovery.Register(); err != nil {
			return err
		}
	}

	d.wg.Add(2)
	go d.eventLoop(eventsCh) // 处理 ml 事件，转发给 subscribers ，即上面两个管道 d.ClusterEvents/d.deadMemberEvents
	go d.deadMemberTracker() //
	return nil
}

// Join is used to take an existing Memberlist and attempt to Join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Memberlist only contains our own state, so doing this will cause remote
// nodes to become aware of the existence of this node, effectively joining the cluster.
//
// Join 通过联系其他集群节点，进行状态同步，使其他节点知晓当前节点的存在，从而完成加入集群的操作。
// 如果加入集群成功，成功返回加入的节点数量，失败则报错。
//
// 如果 d.serviceDiscovery 存在，调用 DiscoverPeers() 方法来发现其他可用节点的地址列表。
// d.serviceDiscovery 是一个接口或对象，用于动态发现集群中的其他节点。
//
// 如果成功获取到其他节点 peers，调用 d.memberlist.Join(peers) 将当前节点加入这些节点所在的集群。
// Join 方法会尝试联系 peers 中的所有节点，并进行状态同步，使得它们能够知晓当前节点的加入。
//
// 如果 d.serviceDiscovery 不存在，则尝试使用 d.config.Peers 中定义的节点列表来加入集群。
func (d *Discovery) Join() (int, error) {
	if d.serviceDiscovery != nil {
		peers, err := d.serviceDiscovery.DiscoverPeers()
		if err != nil {
			return 0, err
		}
		return d.memberlist.Join(peers)
	}
	return d.memberlist.Join(d.config.Peers)
}

func (d *Discovery) Rejoin(peers []string) (int, error) {
	return d.memberlist.Join(peers)
}

// GetMembers returns a full list of known alive nodes.
//
// 返回 ml 集群中所有节点，按照启动时间排序。
//
// 获取所有节点
// 解析节点元数据，得到 Member<节点名,节点ID,启动时间>
// 按照启动时间排序后返回
func (d *Discovery) GetMembers() []Member {
	var members []Member
	nodes := d.memberlist.Members()
	for _, node := range nodes {
		member, _ := d.DecodeNodeMeta(node.Meta)
		members = append(members, member)
	}
	// sort members by birthdate
	sort.Slice(members, func(i int, j int) bool {
		return members[i].Birthdate < members[j].Birthdate
	})
	return members
}

func (d *Discovery) NumMembers() int {
	return d.memberlist.NumMembers()
}

// FindMemberByName finds and returns an alive member.
func (d *Discovery) FindMemberByName(name string) (Member, error) {
	members := d.GetMembers()
	for _, member := range members {
		if member.Name == name {
			return member, nil
		}
	}
	return Member{}, ErrHostNotFound
}

// FindMemberByID finds and returns an alive member.
func (d *Discovery) FindMemberByID(id uint64) (Member, error) {
	members := d.GetMembers()
	for _, member := range members {
		if member.ID == id {
			return member, nil
		}
	}
	return Member{}, ErrHostNotFound
}

// GetCoordinator returns the oldest node in the memberlist.
//
// 返回 ml 集群中所有节点，按照启动时间排序
// 启动时间最早的 node 为 coordinator
func (d *Discovery) GetCoordinator() Member {
	members := d.GetMembers()
	if len(members) == 0 {
		d.log.V(1).Printf("[ERROR] There is no member in memberlist")
		return Member{}
	}
	return members[0]
}

// IsCoordinator returns true if the caller is the coordinator node.
func (d *Discovery) IsCoordinator() bool {
	return d.GetCoordinator().ID == d.host.ID
}

// LocalNode is used to return the local Node
func (d *Discovery) LocalNode() *memberlist.Node {
	return d.memberlist.LocalNode()
}

// Shutdown will stop any background maintenance of network activity
// for this memberlist, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's Shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// This method is safe to call multiple times.
func (d *Discovery) Shutdown() error {
	select {
	case <-d.ctx.Done():
		return nil
	default:
	}
	d.cancel()
	// We don't do that in a goroutine with a timeout mechanism
	// because this mechanism may cause goroutine leak.
	d.wg.Wait()

	// Leave will broadcast a leave message but will not shutdown the background
	// listeners, meaning the node will continue participating in gossip and state
	// updates.
	d.log.V(2).Printf("[INFO] Broadcasting a leave message")
	if err := d.memberlist.Leave(15 * time.Second); err != nil {
		d.log.V(3).Printf("[ERROR] memberlist.Leave returned an error: %v", err)
	}

	if d.serviceDiscovery != nil {
		defer d.serviceDiscovery.Close()
		if err := d.serviceDiscovery.Deregister(); err != nil {
			d.log.V(3).Printf("[ERROR] ServiceDiscovery.Deregister returned an error: %v", err)
		}
	}
	return d.memberlist.Shutdown()
}

func convertToClusterEvent(e memberlist.NodeEvent) *ClusterEvent {
	return &ClusterEvent{
		Event:    e.Event,     // 事件类型：Join/Leave/Update
		NodeName: e.Node.Name, // 节点名
		NodeAddr: e.Node.Addr, // 地址
		NodePort: e.Node.Port, // 端口
		NodeMeta: e.Node.Meta, // 元数据
	}
}

// 把 ml 事件转发给 subscribers
func (d *Discovery) handleEvent(event memberlist.NodeEvent) {
	d.clusterEventsMtx.RLock()
	defer d.clusterEventsMtx.RUnlock()

	for _, ch := range d.eventSubscribers {
		// 如果 ml 事件是本机产生的，忽略
		if event.Node.Name == d.host.Name {
			continue
		}
		// 如果 ml 事件是 NodeJoin or NodeLeave ，转发给 Subscriber
		if event.Event != memberlist.NodeUpdate {
			ch <- convertToClusterEvent(event)
			continue
		}
		// 如果 ml 事件是 NodeUpdate ，转换为先 NodeLeave 再 NodeJoin 两个事件；
		// NodeUpdate: Olric is an in-memory k/v store. If the node metadata has been updated,
		// the node may be restarted or/and serves stale data.
		e := convertToClusterEvent(event)
		e.Event = memberlist.NodeLeave
		ch <- e
		// Create a Join event from copied event.
		e = convertToClusterEvent(event)
		e.Event = memberlist.NodeJoin
		ch <- e
	}
}

// eventLoop awaits for messages from memberlist and broadcasts them to  event listeners.
//
// 处理 ml 事件，转发给 subscribers
func (d *Discovery) eventLoop(eventsCh chan memberlist.NodeEvent) {
	defer d.wg.Done()
	for {
		select {
		case e := <-eventsCh:
			d.handleEvent(e)
		case <-d.ctx.Done():
			return
		}
	}
}

// SubscribeNodeEvents 注册 ml 事件的 subscriber
func (d *Discovery) SubscribeNodeEvents() chan *ClusterEvent {
	d.clusterEventsMtx.Lock()
	defer d.clusterEventsMtx.Unlock()
	ch := make(chan *ClusterEvent, eventChanCapacity)
	d.eventSubscribers = append(d.eventSubscribers, ch)
	return ch
}

// 通过 ml.Delegate 可以在节点之间传递自定义的元数据和处理用户数据消息。
// 这个接口允许开发者在节点加入、离开或更新时，执行一些自定义的逻辑。
//
// Delegate 接口定义了以下方法：
//	NodeMeta: 返回一个字节数组，代表当前节点的元数据。这些元数据会在节点加入集群时广播给其他节点。
//	NotifyMsg: 当节点接收到用户数据消息时调用。用户可以通过这个方法处理接收到的消息。
//	GetBroadcasts: 返回需要广播给其他节点的用户数据消息。可以通过这个方法向其他节点发送自定义消息。
//	LocalState: 返回当前节点的本地状态数据。当节点加入集群或进行状态同步时调用。
//	MergeRemoteState: 当接收到其他节点的状态数据时调用。用于将远程节点的状态数据合并到本地状态中。

// delegate is a struct which implements memberlist.Delegate interface.
type delegate struct {
	meta []byte // 存储本节点信息：节点名、节点 ID 、启动时间
}

// newDelegate returns a new delegate instance.
func (d *Discovery) newDelegate() (delegate, error) {
	data, err := msgpack.Marshal(d.host)
	if err != nil {
		return delegate{}, err
	}
	return delegate{
		meta: data,
	}, nil
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
//
// 节点加入集群时，会将 meta 广播给集群内各节点
func (d delegate) NodeMeta(limit int) []byte {
	return d.meta
}

// NotifyMsg is called when a user-data message is received.
func (d delegate) NotifyMsg(data []byte) {}

// GetBroadcasts is called when user data messages can be broadcast.
func (d delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState is used for a TCP Push/Pull.
func (d delegate) LocalState(join bool) []byte { return nil }

// MergeRemoteState is invoked after a TCP Push/Pull.
func (d delegate) MergeRemoteState(buf []byte, join bool) {}
