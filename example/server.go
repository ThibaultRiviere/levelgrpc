package main

import (
	levelgrpc "github.com/ThibaultRiviere/levelgrpc/pkg/server"
	"log"
	"net"
)

func main() {

	lis, err := net.Listen("tcp", ":4242")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}

	db, err := levelgrpc.NewServer()
	if err != nil {
		log.Fatalf("failed to create levelgrpc: %v", err)
		return
	}

	db.Serve(lis)
}
