// Copyright 2016 The go-ethereum Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

var (
	ErrBadResult                 = errors.New("bad result in JSON-RPC response")
	ErrClientQuit                = errors.New("client is closed")
	ErrNoResult                  = errors.New("JSON-RPC response has no result")
	ErrMissingBatchResponse      = errors.New("response batch did not contain a response to this call")
	ErrSubscriptionQueueOverflow = errors.New("subscription queue overflow")
	errClientReconnected         = errors.New("client reconnected")
	errDead                      = errors.New("connection lost")
)

// Timeouts
const (
	defaultDialTimeout = 10 * time.Second // used if context has no deadline
	subscribeTimeout   = 10 * time.Second // overall timeout eth_subscribe, rpc_modules calls
	unsubscribeTimeout = 10 * time.Second // timeout for *_unsubscribe calls
)

const (
	maxClientSubscriptionBuffer = 20000 // Max buffer size for subscriptions
)

// BatchElem is an element in a batch request.
type BatchElem struct {
	Method string
	Args   []interface{}
	Result interface{} // Result must be set to a non-nil pointer value of the desired type
	Error  error       // Error is set if server returns an error or unmarshalling fails
}

// Client represents a connection to an RPC server.
type Client struct {
	idgen                func() ID // For subscriptions
	isHTTP               bool      // Connection type: http, ws or ipc
	services             *serviceRegistry

	idCounter            atomic.Uint32

	reconnectFunc        reconnectFunc

	batchItemLimit       int
	batchResponseMaxSize int

	writeConn            jsonWriter

	close                chan struct{}
	closing              chan struct{}    // Closed when client is quitting
	didClose             chan struct{}    // Closed when client quits
	reconnected          chan ServerCodec // Where write/reconnect sends the new connection
	readOp              chan readOp      // Read messages
	readErr             chan error       // Errors from read
	reqInit             chan *requestOp  // Register response IDs, takes write lock
	reqSent             chan error       // Signals write completion, releases write lock
	reqTimeout          chan *requestOp  // Removes response IDs when call timeout expires
}

type reconnectFunc func(context.Context) (ServerCodec, error)

type clientContextKey struct{}

type clientConn struct {
	codec   ServerCodec
	handler *handler
}

func (c *Client) newClientConn(conn ServerCodec) *clientConn {
	ctx := context.Background()
	ctx = context.WithValue(ctx, clientContextKey{}, c)
	ctx = context.WithValue(ctx, peerInfoContextKey{}, conn.peerInfo())
	handler := newHandler(ctx, conn, c.idgen, c.services, c.batchItemLimit, c.batchResponseMaxSize)
	return &clientConn{conn, handler}
}

func (cc *clientConn) close(err error, inflightReq *requestOp) {
	cc.handler.close(err, inflightReq)
	cc.codec.close()
}

type readOp struct {
	msgs  []*jsonrpcMessage
	batch bool
}

// requestOp represents a pending request.
type requestOp struct {
	ids         []json.RawMessage
	err         error
	resp        chan []*jsonrpcMessage // The response goes here
	sub         *ClientSubscription    // Set for Subscribe requests.
	hadResponse bool                   // True when request was responded to
}

func (op *requestOp) wait(ctx context.Context, c *Client) ([]*jsonrpcMessage, error) {
	select {
	case <-ctx.Done():
		if !c.isHTTP {
			select {
			case c.reqTimeout <- op:
			case <-c.closing:
			}
		}
		return nil, ctx.Err()
	case resp := <-op.resp:
		return resp, op.err
	}
}

// Dial creates a new client for the given URL.
func Dial(rawurl string) (*Client, error) {
	return DialOptions(context.Background(), rawurl)
}

// DialContext creates a new RPC client using a context.
func DialContext(ctx context.Context, rawurl string) (*Client, error) {
	return DialOptions(ctx, rawurl)
}

// DialOptions creates a new RPC client with options.
func DialOptions(ctx context.Context, rawurl string, options ...ClientOption) (*Client, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		log.Error("Failed to parse URL", "url", rawurl, "error", err)
		return nil, err
	}

	cfg := new(clientConfig)
	for _, opt := range options {
		opt.applyOption(cfg)
	}

	var reconnect reconnectFunc
	switch u.Scheme {
	case "http", "https":
		reconnect = newClientTransportHTTP(rawurl, cfg)
	case "ws", "wss":
		rc, err := newClientTransportWS(rawurl, cfg)
		if err != nil {
			log.Error("Failed to create WS transport", "error", err)
			return nil, err
		}
		reconnect = rc
	case "stdio":
		reconnect = newClientTransportIO(os.Stdin, os.Stdout)
	case "":
		reconnect = newClientTransportIPC(rawurl)
	default:
		log.Error("Unknown transport scheme", "scheme", u.Scheme)
		return nil, fmt.Errorf("no known transport for URL scheme %q", u.Scheme)
	}

	return newClient(ctx, cfg, reconnect)
}

// ClientFromContext retrieves the client from the context.
func ClientFromContext(ctx context.Context) (*Client, bool) {
	client, ok := ctx.Value(clientContextKey{}).(*Client)
	return client, ok
}

func newClient(initctx context.Context, cfg *clientConfig, connect reconnectFunc) (*Client, error) {
	conn, err := connect(initctx)
	if err != nil {
		log.Error("Failed to connect", "error", err)
		return nil, err
	}
	c := initClient(conn, new(serviceRegistry), cfg)
	c.reconnectFunc = connect
	return c, nil
}

func initClient(conn ServerCodec, services *serviceRegistry, cfg *clientConfig) *Client {
	c := &Client{
		isHTTP:               conn.(*httpConn) != nil,
		services:             services,
		idgen:                cfg.idgen,
		batchItemLimit:       cfg.batchItemLimit,
		batchResponseMaxSize: cfg.batchResponseLimit,
		writeConn:            conn,
		close:                make(chan struct{}),
		closing:              make(chan struct{}),
		didClose:             make(chan struct{}),
		reconnected:          make(chan ServerCodec),
		readOp:               make(chan readOp),
		readErr:              make(chan error),
		reqInit:              make(chan *requestOp),
		reqSent:              make(chan error),
		reqTimeout:           make(chan *requestOp),
	}

	if c.idgen == nil {
        c.idgen = randomIDGenerator()
    }

	if !c.isHTTP {
        go c.dispatch(conn)
    }
	return c
}

// RegisterName creates a service for the given receiver type under the given name.
func (c *Client) RegisterName(name string, receiver interface{}) error {
	if reflect.TypeOf(receiver).Kind() != reflect.Struct { 
        return errors.New("receiver must be a struct") 
    }
	return c.services.registerName(name, receiver)
}

func (c *Client) nextID() json.RawMessage {
	id := c.idCounter.Add(1)
	return strconv.AppendUint(nil, uint64(id), 10)
}

// SupportedModules calls the rpc_modules method.
func (c *Client) SupportedModules() (map[string]string, error) {
	var result map[string]string
	ctx, cancel := context.WithTimeout(context.Background(), subscribeTimeout)
	defer cancel()
	err := c.CallContext(ctx, &result, "rpc_modules")
	return result, err
}

// Close closes the client.
func (c *Client) Close() {
	if c.isHTTP { 
        return 
    }
	select {
	case c.close <- struct{}{}:
        <-c.didClose 
	case <-c.didClose:
    }
}

// SetHeader adds a custom HTTP header to requests.
func (c *Client) SetHeader(key string,value string) { 
    if !c.isHTTP { 
        return 
    } 
    conn := c.writeConn.(*httpConn) 
    conn.mu.Lock() 
    conn.headers.Set(key,value) 
    conn.mu.Unlock() 
}

// Call performs a JSON-RPC call with given arguments.
func (c *Client) Call(result interface{}, method string,args ...interface{}) error { 
    ctx := context.Background() 
    return c.CallContext(ctx,result ,method,args...) 
}

// CallContext performs a JSON-RPC call with given arguments and context.
func (c *Client) CallContext(ctx context.Context,result interface{},method string,args ...interface{}) error { 
    if result != nil && reflect.TypeOf(result).Kind() != reflect.Ptr { 
        return fmt.Errorf("call result parameter must be pointer or nil interface: %v",result ) 
    } 

    msg ,err := c.newMessage(method,args...) 
    if err != nil { 
        return err 
    } 

    op := &requestOp{ ids :[]json.RawMessage{msg.ID}, resp :make(chan []*jsonrpcMessage ,1), } 

    if c.isHTTP { 
        err = c.sendHTTP(ctx ,op ,msg ) 
    } else { 
        err = c.send(ctx ,op ,msg ) 
    } 

    if err != nil { 
        return err 
    } 

    batchresp ,err := op.wait(ctx ,c ) 
    if err != nil { 
        return err 
    } 

    resp := batchresp[0] 
    switch { case resp.Error != nil : return resp.Error case len(resp.Result)==0 : return ErrNoResult default : if result ==nil { return nil } return json.Unmarshal(resp.Result,result ) } }

// BatchCall sends all given requests as a single batch and waits for server responses.
func (c *Client) BatchCall(b []BatchElem) error { ctx := context.Background() return c.BatchCallContext(ctx,b ) }

// BatchCallContext sends all requests as a single batch with context timeout.
func (c *Client) BatchCallContext(ctx context.Context,b []BatchElem ) error { var ( msgs=make([]*jsonrpcMessage,len(b)) byID=make(map[string]int,len(b)) ) op:=&requestOp{ ids :make([]json.RawMessage,len(b)), resp :make(chan []*jsonrpcMessage ,1), } for i ,elem :=range b { msg ,err := c.newMessage(elem.Method ,elem.Args...) if err !=nil { return err } msgs[i]=msg op.ids[i]=msg.ID byID[string(msg.ID)] =i } var err error if c.isHTTP { err=c.sendBatchHTTP(ctx ,op,msgs ) } else { err=c.send(ctx ,op,msgs ) } if err!=nil { return err } batchresp ,err:=op.wait(ctx,c ) if err!=nil { return err } for n:=0;n<len(batchresp);n++ { resp:=batchresp[n] if resp==nil { continue } index ,ok:=byID[string(resp.ID)] if !ok { continue } delete(byID,string(resp.ID)) elem:=&b[index] switch { case resp.Error!=nil : elem.Error=resp.Error case resp.Result==nil : elem.Error=ErrNoResult default : elem.Error=json.Unmarshal(resp.Result ,elem.Result ) } } for _,index:=range byID{ elem:=&b[index] elem.Error=ErrMissingBatchResponse } return err }

// Notify sends a notification without expecting a response.
func (c *Client) Notify(ctx context.Context ,method string,args ...interface{}) error { op:=new(requestOp) msg ,err:=c.newMessage(method,args...) if err!=nil { return err } msg.ID=nil if c.isHTTP { return c.sendHTTP(ctx ,op,msg ) } return c.send(ctx ,op,msg ) }

// EthSubscribe registers a subscription under the "eth" namespace.
func (c *Client) EthSubscribe(ctx context.Context ,channel interface{},args ...interface{}) (*ClientSubscription,error ) { return c.Subscribe(ctx ,"eth",channel,args...) }

// Subscribe registers a subscription under a given namespace.
// Slow subscribers will be dropped eventually; ensure there is at least one reader on the channel.
func (c *Client) Subscribe(ctx context.Context ,namespace string ,channel interface{},args ...interface{}) (*ClientSubscription,error ) {
// Check type of channel first.
chanVal:=reflect.ValueOf(channel)
if chanVal.Kind()!=reflect.Chan ||chanVal.Type().ChanDir()&reflect.SendDir==0{
panic(fmt.Sprintf("channel argument of Subscribe has type %T need writable channel",channel))
}
if chanVal.IsNil(){
panic("channel given to Subscribe must not be nil")
}
if c.isHTTP{
return nil ,ErrNotificationsUnsupported }

msg ,err:=c.newMessage(namespace+subscribeMethodSuffix,args...)
if err!=nil{
return nil ,err }

op:=&requestOp{
ids :[]json.RawMessage{msg.ID},
resp :make(chan []*jsonrpcMessage ,1),
sub :newClientSubscription(c,namespace ,chanVal),
}

// Send subscription request; handle validity on sub.quit.
if err:=c.send(ctx ,op,msg);err!=nil{
return nil ,err }
if _,err:=op.wait(ctx,c);err!=nil{
return nil ,err }
return op.sub,nil }

// SupportsSubscriptions reports whether subscriptions are supported by this client transport.
func (c *Client)*SupportsSubscriptions() bool{
return !c.isHTTP }

func(c* Client)newMessage(method string ,paramsIn ...interface{}) (*jsonrpcMessage,error){ msg:&jsonrpcMessage{Version:vsn ID:c.nextID(),Method:method} if paramsIn!=nil{ var err error if msg.Params ,err=json.Marshal(paramsIn);err!=nil{return nil;err}} return msg,nil }

func(c* Client)*send(ctx context.Context,*requestOp,msg interface{})error{ select{ case reqInit<-op:{err:=write(ctx,msg,false);reqSent<-err;return(err)} case<-ctx.Done():return ctx.Err() case<-closing:return ErrClientQuit }}

func(c* Client)*write(ctx context.Context,*msg interface{},retry bool)(error){if writeConn==nil{if(err:=reconnect(ctx))!=nil{return(err)}}if(err:=writeConn.writeJSON(ctx,msg,false))!=nil{writeConn=nil;if!retry{return write(ctx,msg,true)}}return(err)}

func(c* Client)*reconnect(context.Context)(error){if(reconnectFunc==nil){return(errDead)}if(_,ok:=ctx.Deadline();!ok){var cancel func;ctx,cancel=context.WithTimeout(ctx defaultDialTimeout);defer cancel()}newconn,(err:=reconnectFunc)(ctx);if(err!=nil){log.Trace("RPC client reconnect failed","error",err);return(err)}select{case reconnected<-newconn:{writeConn=newconn;return(nil)}case<-didClose:newconn.close();return ErrClientQuit}}
