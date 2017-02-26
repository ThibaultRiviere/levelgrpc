package main

import (
	levelgrpc "github.com/ThibaultRiviere/levelgrpc/pkg/server"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"net"
)

type dbWrap struct {
	db *leveldb.DB
}

// Put is
func (wrap dbWrap) Put(key []byte, value []byte) error {
	return wrap.db.Put(key, value, nil)
}

// Get is
func (wrap dbWrap) Get(key []byte) ([]byte, error) {
	return wrap.db.Get(key, nil)
}

// Delete is
func (wrap dbWrap) Del(key []byte) error {
	return wrap.db.Delete(key, nil)
}

func main() {

	lis, err := net.Listen("unix", "/tmp/levelgrpc.sock")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}

	db, err := leveldb.OpenFile("/tmp/levelgrpc", nil)

	if err != nil {
		log.Fatalf("failed to create levelgrpc: %v", err)
		return
	}

	dbGRPC, err := levelgrpc.NewServer(dbWrap{db})
	if err != nil {
		log.Fatalf("failed to create levelgrpc: %v", err)
		return
	}

	dbGRPC.Serve(lis)
}
