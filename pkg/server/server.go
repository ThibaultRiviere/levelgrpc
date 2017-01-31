// Package server define the way to communicate with the leveldb using grpc
// It allow multiple processes to communicate to a single leveldb
//
// For communication see proto/level_grpc.proto
package server

import (
	"fmt"
	pb "github.com/ThibaultRiviere/levelgrpc/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
)

// DBInterface is the parameters give for a new levelgrpc
// Levelgrpc will bind theses function from a grpc server
// It is important to note that this interface will have breaking changes
// when levelgrpc is updated and adds new API operations.
// Its suggested to use the pattern above for testing, or using
// tooling to generate mocks to satisfy the interfaces.
type DBInterface interface {
	Put([]byte, []byte) error
	Del([]byte) error
	Get([]byte) ([]byte, error)
}

// Server is a leveldb with grpc as communication.
// It will simply transfering the request to leveldb with the given parameters
// using the public functions are the handler for the grpc level and apply
// the given request to leveldb
type Server struct {
	db     DBInterface
	server *grpc.Server
}

// NewServer will initialize the levelgrpc. First it will open the database
// and then it will initialize a new grpc.Server. Is needed to call the function
// Serve with a net.Listerner for begin to serve requests
func NewServer(db DBInterface) (*Server, error) {
	return &Server{db, grpc.NewServer()}, nil
}

// Del is the grpc handler for delete requests.
// Call the function Delete for DBInterface
func (s *Server) Del(c context.Context, m *pb.DelRequest) (*pb.Response, error) {
	err := s.db.Del(m.GetKey())
	if err != nil {
		fmt.Println("del error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, nil}, nil
}

// Put is the grpc handler for put requests.
// Call the function Put for DBInterface
func (s *Server) Put(c context.Context, m *pb.PutRequest) (*pb.Response, error) {
	err := s.db.Put(m.GetKey(), m.GetValue())
	if err != nil {
		fmt.Println("put error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, nil}, nil
}

// Get is the grpc handler for get requests.
// Call the function Get for DBInterface
func (s *Server) Get(c context.Context, m *pb.GetRequest) (*pb.Response, error) {
	data, err := s.db.Get(m.GetKey())
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

/*
// Close will close the server and the grpc server
func (s *Server) Close() error {
	// TODO Search fpr grpc Close
	return s.db.Close()
}
*/
