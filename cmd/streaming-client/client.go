
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "distributed-pow/gen/logs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(
		"localhost:50052",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewLogServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	// Unary RPC
	info, err := client.GetServerInfo(ctx, &pb.InfoRequest{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("===== UNARY RPC =====")
	fmt.Println("Server:", info.GetServerName())
	fmt.Println("Version:", info.GetVersion())

	// Server-streaming RPC
	fmt.Println("\n===== STREAMING RPC =====")

	stream, err := client.StreamLogs(ctx, &pb.LogRequest{
		Count: 5,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := stream.Recv()

		if err == io.EOF {
			fmt.Println("Stream completed successfully.")
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf(
			"[%s] #%d %s - %s\n",
			msg.GetTimestamp(),
			msg.GetSequence(),
			msg.GetLevel(),
			msg.GetMessage(),
		)
	}
}
