package main

import (
	"fmt"
	"log/slog"
	"net"

	queuev1 "github.com/example/event-gateway/internal/proto/queue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps the gRPC server and its dependencies.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
}

// NewServer creates a gRPC server with the queue service registered.
func NewServer(port string, svc *Service, logger *slog.Logger) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	grpcServer := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(grpcServer, svc)
	reflection.Register(grpcServer)

	return &Server{
		grpcServer: grpcServer,
		listener:   lis,
		logger:     logger,
	}, nil
}

// Serve starts the gRPC server. Blocks until the server stops.
func (s *Server) Serve() error {
	s.logger.Info("gRPC server listening", "addr", s.listener.Addr().String())
	return s.grpcServer.Serve(s.listener)
}

// GracefulStop gracefully shuts down the server.
func (s *Server) GracefulStop() {
	s.logger.Info("shutting down gRPC server")
	s.grpcServer.GracefulStop()
}
