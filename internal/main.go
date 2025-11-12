package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/c12s/starmap/config"
	proto "github.com/c12s/starmap/proto/starchart"
	"github.com/c12s/starmap/repos"
	"github.com/c12s/starmap/services"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {

	cfg := config.GetConfig()

	listener, err := net.Listen("tcp", cfg.ServicePort)
	if err != nil {
		log.Fatalln(err)
	}

	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(listener)

	//Neo4j repo
	repo, err := repos.NewRegistryRepo()
	if err != nil {
		log.Fatalf("Failed to initialize RegistryRepo: %v", err)
	}
	defer repo.Close()

	//Handler
	service := services.NewRegistryService(repo)

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	proto.RegisterRegistryServiceServer(grpcServer, service)

	// Run gRPC server
	go func() {
		log.Println("Starting gRPC server...")
		if err := grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// Gracefully stop the server
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGTERM)
	<-stopCh

	log.Println("Shutting down gRPC server gracefully...")
	grpcServer.GracefulStop()
}
