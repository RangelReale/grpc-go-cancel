package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(h))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := server()
		if err != nil {
			slog.Error("{server} error", "error", err.Error())
		}
		slog.Info("{server} finished")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := client()
		if err != nil {
			slog.Error("[client] error", "error", err.Error())
		}
		slog.Info("[client] finished")
	}()

	wg.Wait()
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *wrappedStream) Context() context.Context {
	return s.ctx
}

func server() error {
	gServer := grpc.NewServer(
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx := context.WithValue(ss.Context(), "x", "12")
			ctx, cancel := context.WithCancelCause(ctx)
			ctx = context.WithValue(ctx, "cancel", cancel)
			return handler(srv, &wrappedStream{ss, ctx})
		}),
	)
	server := &dataServer{}
	RegisterDataServiceServer(gServer, server)

	slog.Info("{server} listening", "port", "18991")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", "18991"))
	if err != nil {
		return err
	}
	return gServer.Serve(listener)
}

type dataServer struct {
}

func (d *dataServer) Handler(handlerServer DataService_HandlerServer) error {
	err := d.handler(handlerServer)
	if err != nil {
		slog.Error("{server} handler returned error", "error", err)
	}
	return err
}

func (d *dataServer) handler(handlerServer DataService_HandlerServer) error {
	slog.Info("{server} client connected")
	defer func() {
		slog.Info("{server} client disconnected")
	}()

	ctx := context.Background()
	ctx, cancel := context.WithCancelCause(ctx)

	interceptorCancel := handlerServer.Context().Value("cancel").(context.CancelCauseFunc)

	dataChan := make(chan *Data)
	var dataErr atomic.Value

	go func() {
		defer close(dataChan)
		for {
			data, err := handlerServer.Recv()
			if errors.Is(err, io.EOF) {
				slog.Debug("{server} [recv] received EOF")
				dataErr.Store(err)
				cancel(err)
				return
			}
			if err != nil {
				slog.Debug("{server} [recv] received error", "error", err)
				dataErr.Store(err)
				cancel(err)
				return
			}
			select {
			case <-handlerServer.Context().Done():
				slog.Debug("{server} [recv] context done", "cause", context.Cause(handlerServer.Context()))
				dataErr.Store(context.Cause(handlerServer.Context()))
				cancel(context.Cause(handlerServer.Context()))
				return
			case dataChan <- data:
			}
		}
	}()

	slog.Debug("[server] context x", "x", handlerServer.Context().Value("x"))

	slog.Debug("[server] sending", "data", "server-test-1")
	err := handlerServer.Send(&Data{Data: "server-test-1"})
	if err != nil {
		slog.Error(err.Error())
	}

	for {
		select {
		case <-handlerServer.Context().Done():
			slog.Debug("{server} handlerServer context done", "cause", context.Cause(handlerServer.Context()))
			return context.Cause(handlerServer.Context())
		case <-ctx.Done():
			slog.Debug("{server} context done", "cause", context.Cause(ctx))
			return context.Cause(ctx)
		case data, ok := <-dataChan:
			if !ok {
				slog.Debug("{server} data chan closed", "cause", dataErr.Load())
				derr := dataErr.Load()
				if derr != nil {
					return derr.(error)
				}
				return nil
			}
			slog.Debug("{server} received data", "data", data.Data)
		case <-time.After(6 * time.Second):
			slog.Debug("{server} interceptor cancel")
			interceptorCancel(errors.New("interceptor cancel"))
		case <-time.After(12 * time.Second):
			return status.Error(codes.FailedPrecondition, "simulated timeout precondition")
		}
	}
}

func client() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancelCause(ctx)

	slog.Info("[client] connecting", "address", "localhost:18991")
	conn, err := grpc.DialContext(ctx, "localhost:18991",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		))
	if err != nil {
		return err
	}
	defer conn.Close()

	slog.Info("[client] connected")

	client := NewDataServiceClient(conn)

	handlerClient, err := client.Handler(ctx)
	if err != nil {
		return err
	}

	// process stream

	dataChan := make(chan *Data)
	var dataErr atomic.Value

	go func() {
		defer close(dataChan)

		for {
			data, err := handlerClient.Recv()
			if errors.Is(err, io.EOF) {
				slog.Debug("[client] [recv] received EOF")
				dataErr.Store(err)
				cancel(err)
				return
			}
			if err != nil {
				slog.Debug("[client] [recv] received error", "error", err)
				dataErr.Store(err)
				cancel(err)
				return
			}
			select {
			case <-handlerClient.Context().Done():
				slog.Debug("[client] [recv] context done", "cause", context.Cause(handlerClient.Context()))
				dataErr.Store(context.Cause(handlerClient.Context()))
				cancel(context.Cause(handlerClient.Context()))
				return
			case dataChan <- data:
			}
		}
	}()

	slog.Info("[client] sending", "data", "test1")
	err = handlerClient.Send(&Data{Data: "test1"})
	if err != nil {
		slog.Error(err.Error())
	}

	for {
		select {
		case <-ctx.Done():
			slog.Debug("[client] context done", "cause", context.Cause(ctx))
			return context.Cause(ctx)
		case data, ok := <-dataChan:
			if !ok {
				slog.Debug("[client] data chan closed", "cause", dataErr.Load())
				derr := dataErr.Load()
				if derr != nil {
					return derr.(error)
				}
				return nil
			}
			slog.Debug("[client] received data", "data", data.Data)
		case <-time.After(3 * time.Second):
			_ = handlerClient.CloseSend()
			// 	cancel(errors.New("client self-cancel"))
		}
	}
}
