package server

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/levelgrpc/proto"
	"net"
)

type Server struct {
	db     *leveldb.DB
	server *grpc.Server
}

func (s *Server) _put(db string, key string, value string) error {
	k := db + "/" + key
	//fmt.Println("Putting : ", k, ":", value)
	return s.db.Put([]byte(k), []byte(value), nil)
}

func (s *Server) _get(db string, key string) (string, error) {
	k := db + "/" + key
	val, err := s.db.Get([]byte(k), nil)
	if err != nil {
		return "", err
	}
	//fmt.Println("Getting : ", key, ":", value)
	return string(val), nil
}

func (s *Server) Put(c context.Context, m *pb.PutObject) (*pb.Response, error) {
	err := s._put(m.GetDatabase(), m.GetKey(), m.GetValue())
	if err != nil {
		fmt.Println("put error : ", err)
		return &pb.Response{"nok", ""}, err
	}
	return &pb.Response{"ok", ""}, nil
}

func (s *Server) Get(c context.Context, m *pb.GetObject) (*pb.Response, error) {
	data, err := s._get(m.GetDatabase(), m.GetKey())
	if err != nil {
		fmt.Println("get error : ", err)
		return &pb.Response{"nok", ""}, err
	}
	return &pb.Response{"ok", data}, nil
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
