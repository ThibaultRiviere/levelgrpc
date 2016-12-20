package main

import (
	"flag"
	"fmt"
	str "github.com/dchest/uniuri"
	pb "google.golang.org/grpc/examples/levelgrpc/client"
	"strconv"
)

func benchGetObject(key string, nb_parallel int, nb_reqs int) {

	end := make(chan string, nb_parallel)

	c, err := pb.NewClient()
	if err != nil {
		end <- "error can't create client with leveldb server"
		return
	}

	for i := 0; i < nb_parallel; i++ {
		go func() {
			for i := 0; i < nb_reqs; i++ {
				key := []byte(key + strconv.Itoa(i))
				_, err := c.GetObject(key)
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

func benchPutObject(key string, nb_parallel int, nb_reqs int, size int) {

	end := make(chan string, nb_parallel)
	value := []byte(str.NewLen(size))

	c, err := pb.NewClient()
	if err != nil {
		end <- "error can't create client with leveldb server"
		return
	}

	for i := 0; i < nb_parallel; i++ {
		go func() {
			for i := 0; i < nb_reqs; i++ {
				key := []byte(key + strconv.Itoa(i))
				err := c.PutObject(key, value)
				if err != nil {
					end <- "error can't get: " + string(key)
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
	cmd := flag.String("c", "unknow", "command to execute")
	parallel := flag.String("p", "1", "parallel reqs")
	requests := flag.String("r", "1", "nb reqs")
	size := flag.String("s", "8", "size of the string for put bench")
	key := flag.String("k", "key/", "base for the key")

	flag.Parse()

	nb_parallel, _ := strconv.Atoi(*parallel)
	nb_reqs, _ := strconv.Atoi(*requests)
	val_size, _ := strconv.Atoi(*size)

	switch *cmd {
	case "get":
		benchGetObject(*key, nb_parallel, nb_reqs)
	case "put":
		benchPutObject(*key, nb_parallel, nb_reqs, val_size)
	default:
		fmt.Println("Usage: <cmd> <db> <key> | <value>")
	}
}
