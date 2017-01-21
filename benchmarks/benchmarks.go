// Package use for benchmarks easly the levelgrpc server
// usage:
//
// benchmarks
//
//	 -c=<cmd> (put|get|del)
//	 -p=<parallel requests>
//	 -r=<numbers of requests>
//	 -s=<size of the value> (use for put operations)
//	 -k=<prefix for the key> (will be use for listing)
//
// exemples:
// 	create two paralleles requests who will execute 1000 times the request del
// 	benchmarks -c=del -r=1000 -p=2
//
// 	create two paralleles requests who will execute 1000 times the request put
// 	benchmarks -c=put -r=1000 -p=2 -s=1024
// operation with a value of size equal to 1024
package main

import (
	"flag"
	"fmt"
	pb "github.com/ThibaultRiviere/levelgrpc/pkg/client"
	str "github.com/dchest/uniuri"
	"strconv"
)

func benchGetObject(key string, nbParallel int, nbReqs int) {

	end := make(chan string, nbParallel)

	for i := 0; i < nbParallel; i++ {
		client, err := pb.NewClient()
		if err != nil {
			end <- "error can't create client with leveldb server"
			return
		}

		go func(c pb.Client, k string, e chan string, max int) {
			for i := 0; i < max; i++ {
				keyI := []byte(k + strconv.Itoa(i))
				_, err := c.GetObject(keyI)
				if err != nil {
					e <- "error can't get :" + strconv.Itoa(i)
					return
				}
			}
			e <- "No errors ..."
		}(client, key, end, nbReqs)
	}
	for i := 0; i < nbParallel; i++ {
		fmt.Println(<-end)
	}
}

func benchPutObject(key string, nbParallel int, nbReqs int, size int) {

	end := make(chan string, nbParallel)
	value := []byte(str.NewLen(size))

	for i := 0; i < nbParallel; i++ {
		client, err := pb.NewClient()
		if err != nil {
			end <- "error can't create client with leveldb server"
			return
		}

		go func(c pb.Client, k string, v []byte, e chan string, max int) {
			for i := 0; i < max; i++ {
				keyI := []byte(k + strconv.Itoa(i))
				err := c.PutObject(keyI, v)
				if err != nil {
					e <- "error can't get: " + string(key)
					return
				}
			}
			e <- "No errors ..."
		}(client, key, value, end, nbReqs)
	}
	for i := 0; i < nbParallel; i++ {
		fmt.Println(<-end)
	}
}

func benchDelObject(key string, nbParallel int, nbReqs int) {

	end := make(chan string, nbParallel)

	for i := 0; i < nbParallel; i++ {
		client, err := pb.NewClient()
		if err != nil {
			end <- "error can't create client with leveldb server"
			return
		}

		go func(c pb.Client, k string, e chan string, max int) {
			for i := 0; i < max; i++ {
				keyI := []byte(k + strconv.Itoa(i))
				err := c.DelObject(keyI)
				if err != nil {
					e <- "error can't get :" + strconv.Itoa(i)
					return
				}
			}
			e <- "No errors ..."
		}(client, key, end, nbReqs)
	}
	for i := 0; i < nbParallel; i++ {
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

	nbParallel, _ := strconv.Atoi(*parallel)
	nbReqs, _ := strconv.Atoi(*requests)
	valSize, _ := strconv.Atoi(*size)

	switch *cmd {
	case "get":
		benchGetObject(*key, nbParallel, nbReqs)
	case "put":
		benchPutObject(*key, nbParallel, nbReqs, valSize)
	case "del":
		benchDelObject(*key, nbParallel, nbReqs)
	default:
		fmt.Println("Usage: <cmd> <db> <key> | <value>")
	}
}
