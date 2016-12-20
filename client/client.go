package client

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/levelgrpc/proto"
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

func (c *Client) GetObject(database *string, key *string) (string, error) {
	a := context.Background()
	b := &pb.GetObject{*database, *key}

	res, err := c.conn.Get(a, b)

	if err != nil {
		log.Fatalf("did not get: %v", err)
		return "", err
	}
	return res.GetValue(), nil
}

func (c *Client) PutObject(database *string, key *string, value *string) error {
	a := context.Background()
	b := &pb.PutObject{*database, *key, *value}

	_, err := c.conn.Put(a, b)
	return err
}
