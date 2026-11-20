
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "distributed-pow/gen/logs"

	"google.golang.org/grpc"
)

type logServer struct {
	pb.UnimplementedLogServiceServer
}

func (s *logServer) GetServerInfo(
	ctx context.Context,
	req *pb.InfoRequest,
) (*pb.InfoResponse, error) {
	return &pb.InfoResponse{
		ServerName: "Distributed Systems Log Server",
		Version:    "1.0",
	}, nil
}

func (s *logServer) StreamLogs(
	req *pb.LogRequest,
	stream grpc.ServerStreamingServer[pb.LogMessage],
) error {
	count := req.GetCount()

	if count < 1 || count > 100 {
		return fmt.Errorf("count must be between 1 and 100")
	}

	for i := int32(1); i <= count; i++ {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}

		msg := &pb.LogMessage{
			Sequence:  i,
			Level:     "INFO",
			Message:   fmt.Sprintf("Processing event number %d", i),
			Timestamp: time.Now().Format(time.RFC3339),
		}

		if err := stream.Send(msg); err != nil {
			return err
		}

		log.Printf("Sent log %d/%d", i, count)

		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(time.Second):
		}
	}

	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()
	pb.RegisterLogServiceServer(server, &logServer{})

	log.Println("Log gRPC server listening on :50052")

	if err := server.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
