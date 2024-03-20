package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := server()
		if err != nil {
			slog.Error("server: error", "error", err.Error())
		}
		slog.Info("server: finished")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := client()
		if err != nil {
			slog.Error("client: error", "error", err.Error())
		}
		slog.Info("client: finished")
	}()

	wg.Wait()
}

type dataServer struct {
}

func (d *dataServer) Handler(handlerServer DataService_HandlerServer) error {
	dataChan := make(chan *Data)

	go func() {
		defer close(dataChan)
		for {
			data, err := handlerServer.Recv()
			if errors.Is(err, io.EOF) {
				slog.Info("server recv: received EOF")
				return
			}
			if err != nil {
				slog.Info("server recv: received error", "error", err)
				return
			}
			select {
			case <-handlerServer.Context().Done():
				slog.Info("server recv: context done", "cause", context.Cause(handlerServer.Context()))
				return
			case dataChan <- data:
			}
		}
	}()

	slog.Info("server: processing started")

	for {
		select {
		case <-handlerServer.Context().Done():
			slog.Info("server: context done", "cause", context.Cause(handlerServer.Context()))
			return context.Cause(handlerServer.Context())
		case data, ok := <-dataChan:
			if !ok {
				return nil
			}
			slog.Info("server: received data", "data", data.Data)
		case <-time.After(4 * time.Second):
			return status.Error(codes.FailedPrecondition, "simulated timeout precondition")
		}
	}
}

func server() error {
	gServer := grpc.NewServer()
	server := &dataServer{}
	RegisterDataServiceServer(gServer, server)

	slog.Info("server: listening", "port", "18991")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", "18991"))
	if err != nil {
		return err
	}
	return gServer.Serve(listener)
}

func client() error {
	ctx := context.Background()

	slog.Info("client: connecting", "address", "localhost:18991")
	conn, err := grpc.DialContext(ctx, "localhost:18991",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := NewDataServiceClient(conn)

	handlerClient, err := client.Handler(ctx)
	if err != nil {
		return err
	}

	// process stream

	dataChan := make(chan *Data)

	go func() {
		defer close(dataChan)

		for {
			data, err := handlerClient.Recv()
			if errors.Is(err, io.EOF) {
				slog.Info("client recv: received EOF")
				return
			}
			if err != nil {
				slog.Info("client recv: received error", "error", err)
				return
			}
			select {
			case <-handlerClient.Context().Done():
				slog.Info("client recv: context done", "cause", context.Cause(handlerClient.Context()))
				return
			case dataChan <- data:
			}
		}
	}()

	slog.Info("client: sending", "data", "test1")

	err = handlerClient.Send(&Data{Data: "test1"})
	if err != nil {
		slog.Error(err.Error())
	}

	slog.Info("client: processing started")

	for {
		select {
		case <-handlerClient.Context().Done():
			slog.Info("client: context done", "cause", context.Cause(handlerClient.Context()))
			return context.Cause(handlerClient.Context())
		case data, ok := <-dataChan:
			if !ok {
				return nil
			}
			slog.Info("client: received data", "data", data.Data)
		}
	}
}
