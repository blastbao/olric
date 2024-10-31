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
	"net"
	"runtime"
	"strconv"

	"github.com/hashicorp/go-sockaddr"
)

// The following functions are mostly extracted from Serf. See setupAgent function in cmd/serf/command/agent/command.go
// Thanks for the extraordinary software.
//
// Source: https://github.com/hashicorp/serf/blob/master/cmd/serf/command/agent/command.go#L204

func addrParts(address string) (string, int, error) {
	// 将地址字符串 address 解析为可用于 TCP 连接的 *TCPAddr 对象，解析过程可能涉及 DNS 查询以将主机名解析为 IP 地址。
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return "", 0, err
	}
	// 返回 IP, Port
	return addr.IP.String(), addr.Port, nil
}

// 从给定的网络地址列表中筛选出一个可以用于绑定的 IP 地址。
func getBindIPFromNetworkInterface(addrs []net.Addr) (string, error) {
	// 遍历输入的地址列表
	for _, a := range addrs {
		// 对于 Windows 系统，地址类型为 *net.IPAddr 。
		// 对于其他系统，地址类型为 *net.IPNet 。
		var addrIP net.IP
		if runtime.GOOS == "windows" {
			// Waiting for https://github.com/golang/go/issues/5395 to use IPNet only
			addr, ok := a.(*net.IPAddr)
			if !ok {
				continue
			}
			addrIP = addr.IP
		} else {
			addr, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			addrIP = addr.IP
		}

		// Skip self-assigned IPs
		//
		// Link-local 地址是仅在本地网络段（链路）上有效的地址。这些地址在不同的网络段之间不进行路由。
		//	IPv4：通常在 169.254.0.0/16 范围内。
		//	IPv6：以 fe80::/10 开头。
		//
		// 跳过的原因
		//	局限性：Link-local 地址仅在本地链路有效，不适合用于全局通信或跨网络的通信。
		//	自动配置：这些地址通常是自动配置的，可能不是网络中其他设备可以访问的稳定地址。
		//	不稳定：Link-local 地址可能会在网络配置变化时改变，不适合作为长期绑定的地址。
		//
		// 因此，在选择可绑定的 IP 地址时，通常会跳过 link-local 地址，确保网络程序的稳定性和可达性。
		//
		// Unicast 是最常用的网络通信方式，适合大多数需要点对点连接的应用场景。
		if addrIP.IsLinkLocalUnicast() {
			continue
		}

		// 这里为什么不判断 IsGlobalUnicast ？
		// 除了 link-local 地址，其他类型的 unicast 地址（包括 global unicast）通常都可以用于绑定。
		// 此外，在某些网络环境中，可能还需要考虑私有地址（如 RFC 1918 地址），这些地址也是 unicast，但不一定是 global。

		// 如何判断地址是 Unicast 还是 Multicast/Broadcast/Anycast ？？
		//
		// IPv4
		//	Unicast：
		//	地址范围：0.0.0.0 - 223.255.255.255（不包括保留地址）
		//	常用子网：A、B、C 类地址
		//
		//	Multicast：
		//	地址范围：224.0.0.0 - 239.255.255.255
		//
		//	Broadcast：
		//	地址：通常是子网内的最后一个地址，例如 192.168.1.255（取决于子网掩码）
		//
		//	Anycast：
		//	没有特定的地址范围，通过路由协议实现，多个节点共享一个 Unicast 地址
		return addrIP.String(), nil
	}
	return "", fmt.Errorf("failed to find usable address for interface")
}

// getBindIP 函数用于根据给定的网络接口和地址，选择或验证一个合适的 IP 地址用于绑定。
// 它会处理 0.0.0.0 的特殊情况，并确保返回的 IP 地址在指定的网络接口中可用。
func getBindIP(ifname, address string) (string, error) {

	// 解析网络地址得到 IP
	bindIP, _, err := addrParts(address)
	if err != nil {
		return "", fmt.Errorf("invalid BindAddr: %w", err)
	}

	// Check if we have an interface
	// 检查是否提供了网络接口 ifname
	if iface, _ := net.InterfaceByName(ifname); iface != nil {
		// 获取网络接口地址
		addrs, err := iface.Addrs()
		if err != nil {
			return "", fmt.Errorf("failed to get interface addresses: %w", err)
		}
		if len(addrs) == 0 {
			return "", fmt.Errorf("interface '%s' has no addresses", ifname)
		}

		// If there is no bind IP, pick an address
		// 如果绑定 IP 是 0.0.0.0，则从网络接口地址中选择一个合适的 IP 来 bind 。
		if bindIP == "0.0.0.0" {
			addr, err := getBindIPFromNetworkInterface(addrs)
			if err != nil {
				return "", fmt.Errorf("ip scan on %s: %w", ifname, err)
			}
			return addr, nil
		} else {
			// If there is a bind IP, ensure it is available
			// 如果提供了具体的绑定 IP，检查该 IP 是否在接口地址列表中。
			for _, a := range addrs {
				addr, ok := a.(*net.IPNet)
				if !ok {
					continue
				}
				if addr.IP.String() == bindIP {
					return bindIP, nil
				}
			}
			return "", fmt.Errorf("interface '%s' has no '%s' address", ifname, bindIP)
		}
	}

	// 默认绑定处理：
	//	如果没有找到网络接口 iface 且绑定 IP 是 0.0.0.0 ，尝试获取一个合适的私有 IP 地址，找不到报错
	if bindIP == "0.0.0.0" {
		// if we're not bound to a specific IP, let's use a suitable private IP address.
		ipStr, err := sockaddr.GetPrivateIP()
		if err != nil {
			return "", fmt.Errorf("failed to get interface addresses: %w", err)
		}
		if ipStr == "" {
			return "", fmt.Errorf("no private IP address found, and explicit IP not provided")
		}
		parsed := net.ParseIP(ipStr)
		if parsed == nil {
			return "", fmt.Errorf("failed to parse private IP address: %q", ipStr)
		}
		bindIP = parsed.String()
	}

	return bindIP, nil
}

// 构造 olric bind addr
// 构造 memberlist bind addr

// SetupNetworkConfig tries to find an appropriate bindIP to bind and propagate.
func (c *Config) SetupNetworkConfig() (err error) {
	address := net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))
	c.BindAddr, err = getBindIP(c.Interface, address)
	if err != nil {
		return err
	}

	address = net.JoinHostPort(c.MemberlistConfig.BindAddr, strconv.Itoa(c.MemberlistConfig.BindPort))
	c.MemberlistConfig.BindAddr, err = getBindIP(c.MemberlistInterface, address)
	if err != nil {
		return err
	}

	// AdvertiseAddr 和 BindAddr 在 MemberlistConfig 中有不同的用途，主要区别和联系如下：
	//
	//	BindAddr
	//	用途：BindAddr 是 Olric 节点在本地网络接口上绑定的地址。它用于监听来自其他节点的连接和请求。
	//	使用场景：当节点启动时，它会在这个地址上等待其他节点的连接。
	//
	//	AdvertiseAddr
	//	用途：AdvertiseAddr 是节点向其他节点公布的地址。其他节点使用这个地址与该节点通信。
	//	使用场景：在某些网络环境中（如 NAT 或 Docker），节点的监听地址和其他节点访问它的地址可能不同。
	//	在这种情况下，AdvertiseAddr 用于确保其他节点能够正确访问。
	//
	//	联系
	//	两者可以相同，但在网络复杂的环境中（如有防火墙、NAT 或容器化环境），它们可能不同。
	//	AdvertiseAddr 提供了灵活性，允许节点在不同的网络拓扑中正确地相互发现和通信。
	//	通过分开配置，Olric 可以适应各种网络环境，确保节点之间的通信顺畅。
	if c.MemberlistConfig.AdvertiseAddr != "" {
		advertisePort := c.MemberlistConfig.AdvertisePort
		if advertisePort == 0 {
			advertisePort = c.MemberlistConfig.BindPort
		}
		address := net.JoinHostPort(c.MemberlistConfig.AdvertiseAddr, strconv.Itoa(advertisePort))
		advertiseAddr, _, err := addrParts(address)
		if err != nil {
			return err
		}
		c.MemberlistConfig.AdvertiseAddr = advertiseAddr
	}

	return nil
}
