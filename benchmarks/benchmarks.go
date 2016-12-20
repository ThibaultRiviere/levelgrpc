package main

import (
	"flag"
	"fmt"
	str "github.com/dchest/uniuri"
	pb "google.golang.org/grpc/examples/levelgrpc/client"
	"strconv"
)

func benchGetObject(database *string, key *string, nb_parallel int, nb_reqs int) {

	end := make(chan string, nb_parallel)

	c, err := pb.NewClient()
	if err != nil {
		end <- "error can't create client with leveldb server"
		return
	}

	for i := 0; i < nb_parallel; i++ {
		go func() {
			for i := 0; i < nb_reqs; i++ {
				k := *key + strconv.Itoa(i)
				_, err := c.GetObject(database, &k)
				if err != nil {
					end <- "error can't get :" + strconv.Itoa(i)
					return
				}
			}
			end <- "No errors ..."
		}()
	}
	for i := 0; i < nb_parallel; i++ {
		fmt.Println(<-end)
	}
}

func benchPutObject(d *string, k *string, nb_parallel int, nb_reqs int, size int) {

	end := make(chan string, nb_parallel)
	value := str.NewLen(size)

	c, err := pb.NewClient()
	if err != nil {
		end <- "error can't create client with leveldb server"
		return
	}

	for i := 0; i < nb_parallel; i++ {
		go func() {
			for i := 0; i < nb_reqs; i++ {
				key := *k + strconv.Itoa(i)
				err := c.PutObject(d, &key, &value)
				if err != nil {
					end <- "error can't get: " + key
					return
				}
			}
			end <- "No errors ..."
		}()
	}
	for i := 0; i < nb_parallel; i++ {
		fmt.Println(<-end)
	}
}

func main() {
	cmd := flag.String("cmd", "unknow", "command to execute")
	db := flag.String("db", "foo", "database name")
	key := flag.String("key", "bar", "key name")
	parallel := flag.String("p", "1", "parallel reqs")
	requests := flag.String("req", "1", "nb reqs")
	size := flag.String("s", "8", "size of the string for put bench")

	flag.Parse()

	nb_parallel, _ := strconv.Atoi(*parallel)
	nb_reqs, _ := strconv.Atoi(*requests)
	val_size, _ := strconv.Atoi(*size)

	switch *cmd {
	case "get":
		benchGetObject(db, key, nb_parallel, nb_reqs)
	case "put":
		benchPutObject(db, key, nb_parallel, nb_reqs, val_size)
	default:
		fmt.Println("Usage: <cmd> <db> <key> | <value>")
	}
}
