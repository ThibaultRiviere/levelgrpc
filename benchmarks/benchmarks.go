package main

import (
	"flag"
	"fmt"
	pb "github.com/ThibaultRiviere/levelgrpc/client"
	str "github.com/dchest/uniuri"
	"strconv"
)

func benchGetObject(key string, nb_parallel int, nb_reqs int) {

	end := make(chan string, nb_parallel)

	for i := 0; i < nb_parallel; i++ {
		client, err := pb.NewClient()
		if err != nil {
			end <- "error can't create client with leveldb server"
			return
		}

		go func(c pb.Client, k string, e chan string, max int) {
			for i := 0; i < max; i++ {
				key_i := []byte(k + strconv.Itoa(i))
				_, err := c.GetObject(key_i)
				if err != nil {
					e <- "error can't get :" + strconv.Itoa(i)
					return
				}
			}
			e <- "No errors ..."
		}(client, key, end, nb_reqs)
	}
	for i := 0; i < nb_parallel; i++ {
		fmt.Println(<-end)
	}
}

func benchPutObject(key string, nb_parallel int, nb_reqs int, size int) {

	end := make(chan string, nb_parallel)
	value := []byte(str.NewLen(size))

	for i := 0; i < nb_parallel; i++ {
		client, err := pb.NewClient()
		if err != nil {
			end <- "error can't create client with leveldb server"
			return
		}

		go func(c pb.Client, k string, v []byte, e chan string, max int) {
			for i := 0; i < max; i++ {
				key_i := []byte(k + strconv.Itoa(i))
				err := c.PutObject(key_i, v)
				if err != nil {
					e <- "error can't get: " + string(key)
					return
				}
			}
			e <- "No errors ..."
		}(client, key, value, end, nb_reqs)
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
