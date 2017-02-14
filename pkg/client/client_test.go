package client

import (
	"errors"
	"fmt"
	levelgrpc "github.com/ThibaultRiviere/levelgrpc/pkg/server"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
	"log"
	"net"
	"testing"
	"time"
)

// rpcError defines the status from an RPC.
type rpcError struct {
	code uint32
	desc string
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc error: code = %d desc = %s", e.code, e.desc)
}

var (
	errMem = errors.New("this is an error")
	errRpc = &rpcError{2, "this is an error"}
)

type memServer struct{}

func (mem memServer) Put(key []byte, value []byte) error {
	if string(key) == "error" {
		return errMem
	}
	return nil
}
func (mem memServer) Get(key []byte) ([]byte, error) {
	if string(key) == "error" {
		return nil, errMem
	}
	return nil, nil
}

func (mem memServer) Del(key []byte) error {
	if string(key) == "error" {
		return errMem
	}
	return nil
}

func _runServer(addr string, ready chan bool, end chan bool, stopped chan bool) {
	lis, err := net.Listen("unix", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}

	dbGRPC, err := levelgrpc.NewServer(memServer{})
	if err != nil {
		log.Fatalf("failed to create levelgrpc: %v", err)
		return
	}

	go dbGRPC.Serve(lis)
	ready <- true

	<-end
	lis.Close()
	stopped <- true
}

func _initServer(addr string, close chan bool, stopped chan bool) {
	ready := make(chan bool)
	go _runServer(addr, ready, close, stopped)
	<-ready
}

func getUnixSocket(addr string, t time.Duration) (net.Conn, error) {
	return net.Dial("unix", addr)
}

func _testClientConnection(addr string, errExpected bool, opts ...grpc.DialOption) {
	_, errRet := NewClient(addr, opts...)
	if errExpected == true {
		So(errRet, ShouldNotEqual, nil)
	} else {
		So(errRet, ShouldEqual, nil)
	}
}

func _testApi(f func() error, errExpected bool, err error) {
	ret := f()
	if errExpected == true {
		So(ret.Error(), ShouldEqual, errRpc.Error())
	} else {
		So(ret, ShouldEqual, err)
	}
}

func _testGetApi(cli *Client, errExpected bool) {
	ret := errMem
	key := []byte("error")

	if errExpected == false {
		key = []byte("key")
		ret = nil
	}
	get := func() error {
		_, err := cli.GetObject(key)
		return err
	}
	_testApi(get, errExpected, ret)
}

func _testConnectionError() {
	Convey("without running server should return an error", func() {
		_testClientConnection(
			"localhost:4242",
			true,
			grpc.WithBlock(),
			grpc.FailOnNonTempDialError(true),
			grpc.WithInsecure(),
		)
	})
}

func _testConnectionSuccess() {
	Convey("With a running server should not return an error", func() {
		stop := make(chan bool)
		stopped := make(chan bool)
		addr := "./connectionsuccess.sock"
		_initServer(addr, stop, stopped)

		_testClientConnection(
			addr,
			false,
			grpc.WithDialer(getUnixSocket),
			grpc.WithInsecure(),
		)
		stop <- true
		<-stopped
	})
}

func TestGetClientApi(t *testing.T) {
	Convey("Testing client api get", t, func() {
		stop := make(chan bool)
		stopped := make(chan bool)
		addr := "./apiget.sock"
		_initServer(addr, stop, stopped)
		cli, err := NewClient(addr, grpc.WithDialer(getUnixSocket), grpc.WithInsecure())
		So(err, ShouldEqual, nil)

		_testGetApi(cli, false)
		_testGetApi(cli, true)
		stop <- true
		<-stopped
	})
}

func TestPutClientApi(t *testing.T) {
	Convey("Testing client api get", t, func() {
		stop := make(chan bool)
		stopped := make(chan bool)
		addr := "./apiput.sock"
		_initServer(addr, stop, stopped)
		cli, err := NewClient(addr, grpc.WithDialer(getUnixSocket), grpc.WithInsecure())
		So(err, ShouldEqual, nil)

		_testGetApi(cli, false)
		_testGetApi(cli, true)
		stop <- true
		<-stopped
	})
}

func TestClientConnection(t *testing.T) {
	Convey("Testing Client connection", t, func() {
		_testConnectionSuccess()
		_testConnectionError()
	})
}
