
package main

import (
	"flag"
	"log"

	"distributed-pow/internal/worker"
)

func main() {
	id := flag.String("id", "worker-1", "Worker ID")
	port := flag.String("port", "50061", "Worker gRPC port")
	coordinator := flag.String(
		"coordinator",
		"localhost:50051",
		"Coordinator gRPC address",
	)

	flag.Parse()

	address := ":" + *port

	if err := worker.Run(*id, address, *coordinator); err != nil {
		log.Fatal(err)
	}
}
