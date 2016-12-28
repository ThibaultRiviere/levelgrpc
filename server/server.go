// Package server define the way to communicate with the leveldb using grpc
// It allow multiple processes to communicate to a single leveldb
//
// For communication see proto/level_grpc.proto
package server

import (
	"fmt"
	pb "github.com/ThibaultRiviere/levelgrpc/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
)

// Server is a leveldb with grpc as communication.
// It will simply transfering the request to leveldb with the given parameters
// using the public functions are the handler for the grpc level and apply
// the given request to leveldb
type Server struct {
	db     *leveldb.DB
	server *grpc.Server
}

// NewServer will initialize the levelgrpc. First it will open the database
// and then it will initialize a new grpc.Server. Is needed to call the function
// Serve with a net.Listerner for begin to serve requests
func NewServer() (*Server, error) {
	// Open the database
	// TODO need to be configurable
	db, err := leveldb.OpenFile("/tmp/levelgrpc", nil)
	if err != nil {
		fmt.Println("Failed to open the database")
		return nil, err
	}
	return &Server{db, grpc.NewServer()}, nil
}

func (s *Server) _put(key []byte, value []byte) error {
	return s.db.Put(key, value, nil)
}

func (s *Server) _get(key []byte) ([]byte, error) {
	return s.db.Get(key, nil)
}

func (s *Server) _del(key []byte) error {
	return s.db.Delete(key, nil)
}

// Del is the grpc handler for delete requests, will delete a key in the leveldb
func (s *Server) Del(c context.Context, m *pb.DelRequest) (*pb.Response, error) {
	err := s._del(m.GetKey())
	if err != nil {
		fmt.Println("del error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, nil}, nil
}

// Put is the grpc handler for put requests, will add a key in the leveldb
func (s *Server) Put(c context.Context, m *pb.PutRequest) (*pb.Response, error) {
	err := s._put(m.GetKey(), m.GetValue())
	if err != nil {
		fmt.Println("put error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, nil}, nil
}

// Get is the grpc handler for get requests, will get a key in the leveldb
func (s *Server) Get(c context.Context, m *pb.GetRequest) (*pb.Response, error) {
	data, err := s._get(m.GetKey())
	if err != nil {
		fmt.Println("get error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, data}, nil
}

// Serve is the functio use for begin to server requests.
func (s *Server) Serve(lis net.Listener) error {
	pb.RegisterLevelDBServer(s.server, s)
	s.server.Serve(lis)
	return nil
}
