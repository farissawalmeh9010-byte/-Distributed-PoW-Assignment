package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	pb "distributed-pow/gen/pow"
	"distributed-pow/internal/pow"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server implements the WorkerService gRPC interface.
type Server struct {
	pb.UnimplementedWorkerServiceServer
	ID string
}

// Search processes an assigned nonce range and streams progress.
func (s *Server) Search(
	req *pb.SearchRequest,
	stream grpc.ServerStreamingServer[pb.SearchUpdate],
) error {
	if req.GetDifficulty() < 1 || req.GetDifficulty() > 8 {
		return fmt.Errorf("invalid difficulty")
	}

	if req.GetEndNonce() <= req.GetStartNonce() {
		return fmt.Errorf("invalid nonce range")
	}

	log.Printf(
		"Worker %s: job=%s range=[%d,%d)",
		s.ID,
		req.GetJobId(),
		req.GetStartNonce(),
		req.GetEndNonce(),
	)

	var checked uint64
	lastReport := time.Now()

	for nonce := req.GetStartNonce(); nonce < req.GetEndNonce(); nonce++ {

		// Stop promptly when the coordinator cancels the RPC.
		select {
		case <-stream.Context().Done():
			log.Printf(
				"Worker %s: CANCELLED job=%s",
				s.ID,
				req.GetJobId(),
			)
			return stream.Context().Err()

		default:
		}

		hash := pow.Hash(req.GetData(), nonce)
		checked++

		// Check whether the nonce solves the PoW puzzle.
		if pow.Valid(hash, int(req.GetDifficulty())) {
			log.Printf(
				"Worker %s: FOUND nonce=%d job=%s",
				s.ID,
				nonce,
				req.GetJobId(),
			)

			return stream.Send(&pb.SearchUpdate{
				WorkerId:     s.ID,
				JobId:        req.GetJobId(),
				Checked:      checked,
				CurrentNonce: nonce,
				Found:        true,
				Nonce:        nonce,
				Hash:         hash,
				Completed:    true,
				Message:      "solution found",
			})
		}

		// Send periodic progress updates.
		if checked%10000 == 0 ||
			time.Since(lastReport) >= 500*time.Millisecond {

			err := stream.Send(&pb.SearchUpdate{
				WorkerId:     s.ID,
				JobId:        req.GetJobId(),
				Checked:      checked,
				CurrentNonce: nonce,
				Message:      "searching",
			})

			if err != nil {
				return err
			}

			lastReport = time.Now()
		}
	}

	// The assigned range was completely searched.
	log.Printf(
		"Worker %s: range exhausted job=%s",
		s.ID,
		req.GetJobId(),
	)

	return stream.Send(&pb.SearchUpdate{
		WorkerId:  s.ID,
		JobId:     req.GetJobId(),
		Checked:   checked,
		Completed: true,
		Message:   "range exhausted",
	})
}

// Run starts the worker gRPC server, registers it with
// the coordinator, and periodically sends heartbeats.
func Run(id, address, coordinatorAddress string) error {

	// Open the worker's TCP listener.
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer lis.Close()

	// Create the worker gRPC server.
	server := grpc.NewServer()

	pb.RegisterWorkerServiceServer(
		server,
		&Server{ID: id},
	)

	// Connect to the coordinator if configured.
	if coordinatorAddress != "" {

		conn, err := grpc.NewClient(
			coordinatorAddress,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			return err
		}
		defer conn.Close()

		client := pb.NewCoordinatorServiceClient(conn)

		// Register this worker with the coordinator.
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		resp, err := client.RegisterWorker(
			ctx,
			&pb.RegisterWorkerRequest{
				WorkerId: id,
				Address:  "localhost" + address,
			},
		)
		cancel()

		if err != nil {
			return fmt.Errorf(
				"worker registration failed: %w",
				err,
			)
		}

		if !resp.GetSuccess() {
			return fmt.Errorf(
				"worker registration rejected: %s",
				resp.GetMessage(),
			)
		}

		log.Printf(
			"Worker %s REGISTERED with coordinator",
			id,
		)

		// Send a heartbeat every three seconds.
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for range ticker.C {

				ctx, cancel := context.WithTimeout(
					context.Background(),
					2*time.Second,
				)

				reply, err := client.Heartbeat(
					ctx,
					&pb.HeartbeatRequest{
						WorkerId: id,
					},
				)
				cancel()

				if err != nil {
					log.Printf(
						"Worker %s heartbeat error: %v",
						id,
						err,
					)
					continue
				}

				if !reply.GetAcknowledged() {
					log.Printf(
						"Worker %s heartbeat rejected",
						id,
					)
					continue
				}

				log.Printf(
					"Worker %s HEARTBEAT OK",
					id,
				)
			}
		}()
	}

	log.Printf(
		"Worker %s listening at %s",
		id,
		address,
	)

	// Serve gRPC requests.
	err = server.Serve(lis)

	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}

	return nil
}
