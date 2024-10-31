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

package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/flog"
	"github.com/buraksezer/olric/internal/protocol"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

const (
	idleConn uint32 = 0
	busyConn uint32 = 1
)

// pool is good for recycling memory while reading messages from the socket.
var bufferPool = bufpool.New()

// Server implements a concurrent TCP server.
type Server struct {
	bindAddr        string                            // 监听地址
	bindPort        int                               // 监听端口
	keepAlivePeriod time.Duration                     // 连接保活时间
	log             *flog.Logger                      // 日志
	wg              sync.WaitGroup                    // 协程并发管理
	listener        net.Listener                      // 网络连接监听
	dispatcher      func(w, r protocol.EncodeDecoder) // 请求分发处理
	StartCh         chan struct{}                     // 服务器启动通知
	ctx             context.Context                   // 服务器 ctx
	cancel          context.CancelFunc                // 服务器 cancel
}

// NewServer creates and returns a new Server.
func NewServer(bindAddr string, bindPort int, keepalivePeriod time.Duration, logger *flog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		bindAddr:        bindAddr,
		bindPort:        bindPort,
		keepAlivePeriod: keepalivePeriod,
		log:             logger,
		StartCh:         make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (s *Server) SetDispatcher(f func(w, r protocol.EncodeDecoder)) {
	s.dispatcher = f
}

func (s *Server) controlConnLifeCycle(conn io.ReadWriteCloser, connStatus *uint32, done chan struct{}) {
	// Control connection state and close it.
	defer s.wg.Done()

	select {
	case <-s.ctx.Done():
		// The server is down.
		//
		// 如果 server 退出，对于 busy 的 conn ，不直接关闭连接，而是在 5s 内每 100ms 检查一下请求是否完成，
		// 如果 5s 内完成当前请求，则立即关闭；超过 5s 则直接关闭。
		//
		// 备注，这里实现有个问题，如果 server 退出，conn 主循环还在执行，你发现是 idl 到正要关闭时有个微小时间gap，gap中可能有新请求到达。
		//
	case <-done:
		// The main loop is quit. TCP socket may be closed or a protocol error occurred.
		//
		// 如果 conn 已经出错导致处理请求的主循环退出，那么 status 一定是 idle ，不会走到 if 分支，会直接关闭
	}

	// busy: 已经从 conn 中读取到 req ，正在准备 op
	// idle: 已经从 conn 中读取到 req ，执行完 op ，写回了 rsp ，正在等待新的 req
	if atomic.LoadUint32(connStatus) != idleConn {
		s.log.V(3).Printf("[DEBUG] Connection is busy, waiting")
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		// WARNING: I added this context to fix a deadlock issue when an Olric node is being closed.
		// Debugging such an error is pretty hard and it blocks me. Normally I expect that SetDeadline
		// should fix the problem but It doesn't work. I don't know why. But this hack works well.
		//
		// TODO: Make this parametric.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	loop:
		for {
			select {
			// Wait for the current request. When it mark the connection as idle, break the loop.
			case <-ticker.C:
				if atomic.LoadUint32(connStatus) == idleConn {
					s.log.V(3).Printf("[DEBUG] Connection is idle, closing")
					break loop
				}
			case <-ctx.Done():
				s.log.V(3).Printf("[DEBUG] Connection is still in-use. Aborting.")
				break loop
			}
		}
	}

	// Close the connection and quit.
	if err := conn.Close(); err != nil {
		s.log.V(3).Printf("[DEBUG] Failed to close TCP connection: %v", err)
	}
}

func (s *Server) closeStream(req *protocol.StreamMessage, done chan struct{}) {
	defer s.wg.Done()
	defer req.Close()

	select {
	case <-done:
	case <-s.ctx.Done():
	}
}

// processMessage waits for a new request, handles it and returns the appropriate response.
func (s *Server) processMessage(conn io.ReadWriteCloser, connStatus *uint32, done chan struct{}) error {
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	// 从 conn 中读取 msg ，header 直接返回，body 写入到 buf 中
	header, err := protocol.ReadMessage(conn, buf)
	if err != nil {
		return err
	}

	// 根据 header 确定消息类型，生成具体 Req
	var req protocol.EncodeDecoder
	if header.Magic == protocol.MagicDMapReq {
		req = protocol.NewDMapMessageFromRequest(buf)
	} else if header.Magic == protocol.MagicStreamReq {
		req = protocol.NewStreamMessageFromRequest(buf)
		req.(*protocol.StreamMessage).SetConn(conn)
		s.wg.Add(1)
		go s.closeStream(req.(*protocol.StreamMessage), done)
	} else if header.Magic == protocol.MagicPipelineReq {
		req = protocol.NewPipelineMessageFromRequest(buf)
	} else if header.Magic == protocol.MagicSystemReq {
		req = protocol.NewSystemMessageFromRequest(buf)
	} else if header.Magic == protocol.MagicDTopicReq {
		req = protocol.NewDTopicMessageFromRequest(buf)
	} else {
		// TODO: Return a proper error
		return fmt.Errorf("invalid magic")
	}

	// Decode reads the incoming message from the underlying TCP socket and parses
	// 解析 req
	err = req.Decode()
	if err != nil {
		return errors.WithMessage(err, "failed to read request")
	}

	// busy: 已经从 conn 中读取到 req ，正在准备 rsp 并写回 // Mark connection as busy.
	atomic.StoreUint32(connStatus, busyConn)
	// idle: 已经从 conn 中读取到 req ，执行 op ，写回 rsp // Mark connection as idle before start waiting a new request
	defer atomic.StoreUint32(connStatus, idleConn)

	resp := req.Response(nil) // 重置内部 buf ，因为已经解码了，这个 buf 用不到了，用于后续写 resp

	// The dispatcher is defined by olric package and responsible to evaluate the incoming message.
	// 执行请求
	s.dispatcher(resp, req)

	// 编码响应
	err = resp.Encode()
	if err != nil {
		return err
	}
	// 返回响应
	_, err = resp.Buffer().WriteTo(conn)
	return err
}

// processConn waits for requests and calls request handlers to generate a response. The connections are reusable.
func (s *Server) processConn(conn io.ReadWriteCloser) {
	defer s.wg.Done()

	done := make(chan struct{}) // 下面 for 中出错(网络错误/逻辑错误)时，done 会被 close ，从而通知后台 goroutine 退出。
	defer close(done)

	s.wg.Add(1)

	var connStatus uint32 // connStatus is useful for closing the server gracefully.
	go s.controlConnLifeCycle(conn, &connStatus, done)
	for {
		// processMessage waits to read a message from the TCP socket.
		// Then calls its handler to generate a response.
		//
		// 从 conn 中读取 req ，执行 op ，返回 resp
		err := s.processMessage(conn, &connStatus, done)
		if err != nil {
			// The socket probably would have been closed by the client.
			if errors.Cause(err) == io.EOF || errors.Cause(err) == protocol.ErrConnClosed {
				s.log.V(5).Printf("[ERROR] End of the TCP connection: %v", err)
				break
			}
			s.log.V(5).Printf("[ERROR] Failed to process the incoming request: %v", err)
		}
	}
}

// listenAndServe calls Accept on given net.Listener.
func (s *Server) listenAndServe() error {
	close(s.StartCh)

	for {
		// 接受新连接
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				// the server is closed. just quit.
				return nil
			default:
			}
			s.log.V(3).Printf("[DEBUG] Failed to accept TCP connection: %v", err)
			continue
		}

		// 保活
		if s.keepAlivePeriod.Seconds() != 0 {
			// 启用 TCP Keep-Alive 功能，会定期发送探测数据包，以确保连接仍然有效，即使在没有数据传输的情况下也能保持连接。
			err = conn.(*net.TCPConn).SetKeepAlive(true)
			if err != nil {
				return err
			}
			// 设置探测数据包的发送间隔（保活周期），决定了多久发送一次探测包以验证连接的活跃状态。
			err = conn.(*net.TCPConn).SetKeepAlivePeriod(s.keepAlivePeriod)
			if err != nil {
				return err
			}
		}

		// 处理请求、写回响应
		s.wg.Add(1)
		go s.processConn(conn)
	}
}

// ListenAndServe listens on the TCP network address addr.
func (s *Server) ListenAndServe() error {
	defer func() {
		select {
		case <-s.StartCh:
			return
		default:
		}
		close(s.StartCh)
	}()

	// 创建 listen fd
	addr := net.JoinHostPort(s.bindAddr, strconv.Itoa(s.bindPort))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l

	// 启动 listen
	return s.listenAndServe()
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
// Shutdown works by first closing all open listeners, then closing all idle connections,
// and then waiting indefinitely for connections to return to idle and then shut down.
// If the provided context expires before the shutdown is complete, Shutdown returns
// the context's error, otherwise it returns any error returned from closing the Server's
// underlying Listener(s).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ctx.Done():
		// It's already closed.
		return nil
	default:
	}

	var result error
	s.cancel()
	err := s.listener.Close()
	if err != nil {
		result = multierror.Append(result, err)
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err != nil {
			result = multierror.Append(result, err)
		}
	case <-done:
	}
	return result
}
