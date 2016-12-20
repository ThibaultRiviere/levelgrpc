package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/levelgrpc/proto"
	"log"
	"net"
)

type LevelDB struct {
	db *leveldb.DB
}

func (lvl *LevelDB) Put(c context.Context, m *pb.PutObject) (*pb.Response, error) {
	key := m.GetDatabase() + "/" + m.GetKey()
	err := lvl.db.Put([]byte(key), []byte(m.GetValue()), nil)

	//fmt.Println("Putting : ", key, ":", m.GetValue())

	if err != nil {
		fmt.Println("put error : ", err)
		return &pb.Response{"nok", ""}, err
	}
	return &pb.Response{"ok", ""}, nil
}

func (lvl *LevelDB) Get(c context.Context, m *pb.GetObject) (*pb.Response, error) {
	key := m.GetDatabase() + "/" + m.GetKey()
	data, err := lvl.db.Get([]byte(key), nil)

	if err != nil {
		fmt.Println("get error : ", err)
		return &pb.Response{"nok", ""}, err
	}
	value := string(data)
	//fmt.Println("Getting : ", key, ":", value)
	return &pb.Response{"ok", value}, nil
}

func NewLevelDB() (*LevelDB, error) {
	// Open the database
	// TODO need to be configurable
	db, err := leveldb.OpenFile("/tmp/levelgrpc", nil)
	if err != nil {
		fmt.Println("Failed to open the database")
		return nil, err
	}
	return &LevelDB{db}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":4242")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}
	grpcServer := grpc.NewServer()
	levelgrpc, err := NewLevelDB()
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}
	pb.RegisterLevelDBServer(grpcServer, levelgrpc)
	grpcServer.Serve(lis)
	return
}
