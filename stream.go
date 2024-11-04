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
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
)

// streams maps StreamIDs to streams
type streams struct {
	mu sync.RWMutex

	m map[uint64]*stream
}

// streams provides a bidirectional communication channel between Olric nodes and clients. It can also be used
// for node-to-node communication.
type stream struct {
	mu sync.RWMutex

	pingReceivedAt int64
	conn           io.ReadWriteCloser
	read           chan protocol.EncodeDecoder
	write          chan protocol.EncodeDecoder
	ctx            context.Context
	cancel         context.CancelFunc
	errGr          errgroup.Group
}

// close calls s.cancel and stops all background goroutines.
func (s *stream) close() {
	s.cancel()
}

func (s *stream) readFromStream(bufCh chan<- protocol.EncodeDecoder) error {
	defer s.cancel()

	f := func() error {
		buf := bufferPool.Get()
		defer bufferPool.Put(buf)

		header, err := protocol.ReadMessage(s.conn, buf)
		if err != nil {
			return err
		}

		var msg protocol.EncodeDecoder
		if header.Magic == protocol.MagicStreamReq {
			// [重要] 这里把 s.conn 赛到 msg 中了
			msg = protocol.NewStreamMessageFromRequest(buf)
			msg.(*protocol.StreamMessage).SetConn(s.conn)
		} else if header.Magic == protocol.MagicDMapReq {
			msg = protocol.NewDMapMessageFromRequest(buf)
		} else if header.Magic == protocol.MagicPipelineReq {
			msg = protocol.NewPipelineMessageFromRequest(buf)
		} else {
			return fmt.Errorf("invalid magic")
		}
		err = msg.Decode()
		if err != nil {
			return err
		}
		bufCh <- msg
		return nil
	}

	for {
		// this is good to manage bufferPool with defer statement
		if err := f(); err != nil {
			return err
		}
	}
}

func (s *stream) readLoop() error {
	defer s.cancel()

	bufCh := make(chan protocol.EncodeDecoder, 1)
	s.errGr.Go(func() error {
		return s.readFromStream(bufCh)
	})

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case buf := <-bufCh:
			if buf.OpCode() == protocol.OpStreamPing {
				s.setPingReceivedAt()
				s.write <- protocol.NewStreamMessage(protocol.OpStreamPong)
			} else {
				s.read <- buf
			}
		}
	}
}

func (s *stream) writeToStream(msg protocol.EncodeDecoder) error {
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	msg.SetBuffer(buf)
	err := msg.Encode()
	if err != nil {
		return err
	}
	_, err = msg.Buffer().WriteTo(s.conn)
	return err
}

func (s *stream) writeLoop() error {
	defer s.cancel()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case msg := <-s.write:
			if err := s.writeToStream(msg); err != nil {
				return err
			}
		}
	}
}

// 记录从 stream 上接收消息的最新时间戳
func (s *stream) setPingReceivedAt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pingReceivedAt = time.Now().UnixNano()
}

// 监测流连接是否存活：每秒钟检查最近 5 秒是否在 stream 上接收到了 ping 信号，若无则认为 stream 已断开。
func (db *Olric) checkStreamAliveness(s *stream, streamID uint64) {
	defer db.wg.Done()

loop:
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-db.ctx.Done():
			return
		case <-time.After(time.Second):
			s.mu.RLock()
			if s.pingReceivedAt == 0 {
				s.mu.RUnlock()
				continue loop
			}
			if s.pingReceivedAt+(5*time.Second).Nanoseconds() <= time.Now().UnixNano() {
				// There is no need to call stream.close method here. The underlying socket is already gone.
				s.cancel()
				db.log.V(4).Printf("[INFO] StreamID: %d is dead", streamID)
			}
			s.mu.RUnlock()
		}
	}
}

// 注册一个 stream ：
//   - 启动活跃监听协程：超过 5s 没有收到消息就认为其断开，清理资源；
//   - 启动写协程：首先返回一个 `StreamCreated` 消息给客户端，然后监听 s.write 管道的消息并写入连接（目前看只在 topic 发布消息时会用到）
//   - 启动读协程：将读到的消息投递到 s.read 管道（目前看有用到 stream 上发来的自定义消息，只是读取了 Ping 来更新活跃状态，并返回了 Pong）
func (db *Olric) createStreamOperation(w, r protocol.EncodeDecoder) {
	ctx, cancel := context.WithCancel(context.Background())
	req := r.(*protocol.StreamMessage)
	req.SetCancelFunc(cancel) // this cancel function will be called by the server when the underlying socket is gone.
	s := &stream{
		conn:   req.Conn(),
		read:   make(chan protocol.EncodeDecoder, 1),
		write:  make(chan protocol.EncodeDecoder, 1),
		ctx:    ctx,
		cancel: cancel,
	}

	streamID := rand.Uint64()
	db.streams.mu.Lock()
	db.streams.m[streamID] = s
	db.streams.mu.Unlock()

	db.wg.Add(1)
	go db.checkStreamAliveness(s, streamID)

	defer func() {
		db.streams.mu.Lock()
		delete(db.streams.m, streamID)
		db.streams.mu.Unlock()
		db.log.V(4).Printf("[INFO] StreamID: %d is gone", streamID)
	}()

	rq := protocol.NewStreamMessage(protocol.OpStreamCreated)
	rq.SetExtra(protocol.StreamCreatedExtra{
		StreamID: streamID,
	})
	s.write <- rq

	s.errGr.Go(func() error {
		return s.writeLoop()
	})
	s.errGr.Go(func() error {
		return s.readLoop()
	})
loop:
	for {
		select {
		case <-s.ctx.Done():
			// stream.close method is called
			break loop
		case <-db.ctx.Done():
			// server is gone
			break loop
		}
	}

	// this closes the stream goroutines
	s.cancel()
	if err := s.conn.Close(); err != nil {
		db.log.V(4).Printf("Failed to close underlying TCP socket of StreamID: %d", streamID)
	}

	if err := s.errGr.Wait(); err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
