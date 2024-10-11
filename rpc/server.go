// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"
)

const MetadataApi = "rpc"
const EngineApi = "engine"

// CodecOption specifies which type of messages a codec supports.
type CodecOption int

const (
	// OptionMethodInvocation indicates that the codec supports RPC method calls.
	OptionMethodInvocation CodecOption = 1 << iota

	// OptionSubscriptions indicates that the codec supports RPC notifications.
	OptionSubscriptions // support pub sub
)

// Server is an RPC server.
type Server struct {
	services serviceRegistry
	idgen    func() ID

	mutex              sync.Mutex
	codecs             map[ServerCodec]struct{}
	run                atomic.Bool
	batchItemLimit     int
	batchResponseLimit int
	httpBodyLimit      int
}

// NewServer creates a new server instance with no registered handlers.
func NewServer() *Server {
	server := &Server{
		idgen:         randomIDGenerator(),
		codecs:        make(map[ServerCodec]struct{}),
		httpBodyLimit: defaultBodyLimit,
	}
	server.run.Store(true)

	// Register the default service providing meta information about the RPC service such
	// as the services and methods it offers.
	rpcService := &RPCService{server}
	if err := server.RegisterName(MetadataApi, rpcService); err != nil {
		log.Error("Failed to register RPC service", "error", err)
	}

	return server
}

// SetBatchLimits sets limits applied to batch requests. There are two limits: 'itemLimit'
// is the maximum number of items in a batch. 'maxResponseSize' is the maximum number of
// response bytes across all requests in a batch.
func (s *Server) SetBatchLimits(itemLimit, maxResponseSize int) {
	s.batchItemLimit = itemLimit
	s.batchResponseLimit = maxResponseSize
}

// SetHTTPBodyLimit sets the size limit for HTTP requests.
func (s *Server) SetHTTPBodyLimit(limit int) {
	s.httpBodyLimit = limit
}

// RegisterName creates a service for the given receiver type under the given name.
func (s *Server) RegisterName(name string, receiver interface{}) error {
	return s.services.registerName(name, receiver)
}

// ServeCodec reads incoming requests from codec and writes responses back using it.
func (s *Server) ServeCodec(codec ServerCodec, options CodecOption) {
	defer codec.close()

	if !s.trackCodec(codec) {
		return
	}
	defer s.untrackCodec(codec)

	cfg := &clientConfig{
		idgen:              s.idgen,
		batchItemLimit:     s.batchItemLimit,
		batchResponseLimit: s.batchResponseLimit,
	}
	c := initClient(codec, &s.services, cfg)
	
	select {
	case <-codec.closed():
	case <-c.ctx.Done():
		c.Close()
	}
}

func (s *Server) trackCodec(codec ServerCodec) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.run.Load() {
		return false // Don't serve if server is stopped.
	}
	s.codecs[codec] = struct{}{}
	return true
}

func (s *Server) untrackCodec(codec ServerCodec) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.codecs, codec)
}

// serveSingleRequest reads and processes a single RPC request from the given codec.
func (s *Server) serveSingleRequest(ctx context.Context, codec ServerCodec) {
	if !s.run.Load() {
		return // Don't serve if server is stopped.
	}

	h := newHandler(ctx, codec, s.idgen, &s.services, s.batchItemLimit, s.batchResponseLimit)
	h.allowSubscribe = false
	defer h.close(io.EOF, nil)

	reqs, batch, err := codec.readBatch()
	if err != nil {
		if msg := messageForReadError(err); msg != "" {
			resp := errorMessage(&invalidMessageError{msg})
			codec.writeJSON(ctx, resp, true)
		}
		return
	}

	if batch && len(reqs) > 0 { // Ensure there are requests to handle in batch mode.
		h.handleBatch(reqs)
	} else if len(reqs) > 0 { // Ensure there is at least one request to handle.
		h.handleMsg(reqs[0])
	}
}

func messageForReadError(err error) string {
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "read timeout"
		} else {
			return "read error"
		}
	} else if err != io.EOF {
		return "parse error"
	}
	return ""
}

// Stop stops reading new requests and closes all codecs.
func (s *Server) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.run.CompareAndSwap(true, false) {
		log.Debug("RPC server shutting down")
		
        // Close all codecs to cancel pending requests and subscriptions.
        for codec := range s.codecs {
			codec.close()
			log.Debug("Closed codec", "codec", codec)
        }
    }
}

// RPCService gives meta information about the server.
type RPCService struct {
	server *Server
}

// Modules returns the list of RPC services with their version number.
func (s *RPCService) Modules() map[string]string {
	s.server.services.mu.Lock()
	defer s.server.services.mu.Unlock()

	modules := make(map[string]string)
	for name := range s.server.services.services {
        modules[name] = "1.0" // Assuming version 1.0 for simplicity; adjust as needed.
    }
	return modules
}

// PeerInfo contains information about the remote end of the network connection.
type PeerInfo struct {
    Transport string // Protocol used by client ("http", "ws" or "ipc").
    RemoteAddr string // Client's address (IP address and port).
    HTTP struct { // Additional info for HTTP/WebSocket connections.
        Version string // Protocol version ("HTTP/1.1"). Not set for WebSocket.
        UserAgent string // Client's User-Agent header value.
        Origin string // Origin header value for CORS requests.
        Host string // Host header value sent by client.
    }
}

type peerInfoContextKey struct{}

// PeerInfoFromContext retrieves information about the client's network connection from context.
func PeerInfoFromContext(ctx context.Context) PeerInfo {
	info, _ := ctx.Value(peerInfoContextKey{}).(PeerInfo)
	return info
}
