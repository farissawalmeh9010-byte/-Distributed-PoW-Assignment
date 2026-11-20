package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"time"

	pb "distributed-pow/gen/pow"
	"distributed-pow/internal/coordinator"
	"google.golang.org/grpc"
)

func main() {
	grpcPort := flag.String("grpc-port", "50051", "gRPC port")
	httpPort := flag.String("http-port", "8080", "REST/SSE port")
	rangeSize := flag.Uint64("range-size", 1000000, "Nonces per search range")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 10*time.Second, "Worker heartbeat timeout")
	flag.Parse()
	coord := coordinator.NewConfiguredServer(*rangeSize, *heartbeatTimeout)
	lis, err := net.Listen("tcp", ":"+*grpcPort)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(grpcServer, coord)
	go func() {
		log.Printf("Coordinator gRPC listening on :%s", *grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	log.Printf("Coordinator REST/SSE listening on http://localhost:%s", *httpPort)
	log.Fatal(http.ListenAndServe(":"+*httpPort, coord.HTTPHandler()))
}
