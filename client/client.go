package client

import (
	pb "github.com/ThibaultRiviere/levelgrpc/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
)

const (
	address     = "localhost:4242"
	defaultName = "world"
)

type Client struct {
	conn pb.LevelDBClient
}

// Add ip and port
// TODO need to be configurable
func NewClient() (Client, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return Client{}, err
	}

	return Client{pb.NewLevelDBClient(conn)}, nil
}

func (c *Client) GetObject(key []byte) ([]byte, error) {
	a := context.Background()
	b := &pb.GetRequest{key}

	res, err := c.conn.Get(a, b)

	if err != nil {
		log.Fatalf("did not get: %v", err)
		return nil, err
	}
	return res.GetValue(), nil
}

func (c *Client) PutObject(key []byte, value []byte) error {
	a := context.Background()
	b := &pb.PutRequest{key, value}

	_, err := c.conn.Put(a, b)
	return err
}
