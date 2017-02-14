// Package client define a struct with public function used to communicate
// with the levelgrpc server. It's a wrapper on top of the database.
// It will allow to remove from the usage the comminucation and will act as a
// leveldb.
//
// This wrapper will allow to multiple processus to communication with a single
// database that leveldb doesn't provide, only multiple threads
package client

import (
	pb "github.com/ThibaultRiviere/levelgrpc/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Client will handle the grpc communication with the levelgrpc server,
// emulating an access to a single database
type Client struct {
	conn pb.LevelDBClient
}

// NewClient will initialize the connection with the levelgrpc server
// It will provide the different function of leveldb.
func NewClient(address string, options ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.Dial(address, options...)
	if err != nil {
		return nil, err
	}

	return &Client{pb.NewLevelDBClient(conn)}, nil
}

// GetObject will get through the levelgrpc server the given key
func (c *Client) GetObject(key []byte) ([]byte, error) {
	a := context.Background()
	b := &pb.GetRequest{key}

	res, err := c.conn.Get(a, b)

	if err != nil {
		return nil, err
	}
	return res.GetValue(), nil
}

// PutObject will put through the levelgrpc server the given pair key value
func (c *Client) PutObject(key []byte, value []byte) error {
	a := context.Background()
	b := &pb.PutRequest{key, value}

	_, err := c.conn.Put(a, b)
	return err
}

// DelObject will delete through the levelgrpc server the given key
func (c *Client) DelObject(key []byte) error {
	a := context.Background()
	b := &pb.DelRequest{key}

	_, err := c.conn.Del(a, b)
	return err
}
