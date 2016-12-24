package server

import (
	"fmt"
	pb "github.com/ThibaultRiviere/levelgrpc/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	db     *leveldb.DB
	server *grpc.Server
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

func (s *Server) Del(c context.Context, m *pb.DelRequest) (*pb.Response, error) {
	err := s._del(m.GetKey())
	if err != nil {
		fmt.Println("del error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, nil}, nil
}

func (s *Server) Put(c context.Context, m *pb.PutRequest) (*pb.Response, error) {
	err := s._put(m.GetKey(), m.GetValue())
	if err != nil {
		fmt.Println("put error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, nil}, nil
}

func (s *Server) Get(c context.Context, m *pb.GetRequest) (*pb.Response, error) {
	data, err := s._get(m.GetKey())
	if err != nil {
		fmt.Println("get error : ", err)
		return &pb.Response{true, nil}, err
	}
	return &pb.Response{false, data}, nil
}

func (s *Server) Serve(lis net.Listener) error {
	pb.RegisterLevelDBServer(s.server, s)
	s.server.Serve(lis)
	return nil
}

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
