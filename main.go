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
	allLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	// errorLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	// 	Level: slog.LevelError,
	// }))
	// infoLogger := slog.Default()
	noLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	// serverLogger := slog.Default()
	serverLogger := noLogger
	// clientLogger := slog.Default()
	clientLogger := allLogger

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := server(serverLogger)
		if err != nil {
			serverLogger.Error("{server} error", "error", err.Error())
		}
		serverLogger.Info("{server} finished")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for ct := 0; ct < 100; ct++ {
			err := client(clientLogger)
			if err != nil {
				clientLogger.Error("[client] error", "error", err.Error())
			}
			clientLogger.Info("[client] finished", "ct", ct)
		}
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

func server(logger *slog.Logger) error {
	gServer := grpc.NewServer(
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx := context.WithValue(ss.Context(), "x", "12")
			ctx, cancel := context.WithCancelCause(ctx)
			ctx = context.WithValue(ctx, "cancel", cancel)
			return handler(srv, &wrappedStream{ss, ctx})
		}),
	)
	server := &dataServer{logger: logger}
	RegisterDataServiceServer(gServer, server)

	logger.Info("{server} listening", "port", "18991")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", "18991"))
	if err != nil {
		return err
	}
	return gServer.Serve(listener)
}

type dataServer struct {
	logger *slog.Logger
}

func (d *dataServer) Handler(handlerServer DataService_HandlerServer) error {
	err := d.handler(handlerServer)
	if err != nil {
		d.logger.Error("{server} handler returned error", "error", err)
	}
	return err
}

func (d *dataServer) handler(handlerServer DataService_HandlerServer) error {
	d.logger.Info("{server} client connected")
	defer func() {
		d.logger.Info("{server} client disconnected")
	}()

	// ctx := context.Background()
	ctx := handlerServer.Context()
	ctx, cancel := context.WithCancelCause(ctx)

	// interceptorCancel := handlerServer.Context().Value("cancel").(context.CancelCauseFunc)

	dataChan := make(chan *Data)
	var dataErr atomic.Value

	go func() {
		defer close(dataChan)
		for {
			data, err := handlerServer.Recv()
			if errors.Is(err, io.EOF) {
				d.logger.Debug("{server} [recv] received EOF")
				dataErr.Store(err)
				cancel(err)
				return
			}
			if err != nil {
				d.logger.Debug("{server} [recv] received error", "error", err)
				dataErr.Store(err)
				cancel(err)
				return
			}
			select {
			case <-handlerServer.Context().Done():
				d.logger.Debug("{server} [recv] context done", "cause", context.Cause(handlerServer.Context()))
				dataErr.Store(context.Cause(handlerServer.Context()))
				cancel(context.Cause(handlerServer.Context()))
				return
			case dataChan <- data:
			}
		}
	}()

	d.logger.Debug("[server] context x", "x", handlerServer.Context().Value("x"))

	d.logger.Debug("[server] sending", "data", "server-test-1")
	err := handlerServer.Send(&Data{Data: "server-test-1"})
	if err != nil {
		d.logger.Error(err.Error())
	}

	for {
		select {
		// case <-handlerServer.Context().Done():
		// 	logger.Debug("{server} handlerServer context done", "cause", context.Cause(handlerServer.Context()))
		// 	return context.Cause(handlerServer.Context())
		case <-ctx.Done():
			d.logger.Debug("{server} context done", "cause", context.Cause(ctx))
			return context.Cause(ctx)
		case data, ok := <-dataChan:
			if !ok {
				d.logger.Debug("{server} data chan closed", "cause", dataErr.Load())
				derr := dataErr.Load()
				if derr != nil {
					return derr.(error)
				}
				return nil
			}
			d.logger.Debug("{server} received data", "data", data.Data)
		// case <-time.After(6 * time.Second):
		// 	logger.Debug("{server} interceptor cancel")
		// 	interceptorCancel(errors.New("interceptor cancel"))
		case <-time.After(3 * time.Second):
			cancel(status.Error(codes.FailedPrecondition, "simulated timeout precondition"))
			// return status.Error(codes.FailedPrecondition, "simulated timeout precondition")
		}
	}
}

func client(logger *slog.Logger) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancelCause(ctx)

	logger.Info("[client] connecting", "address", "localhost:18991")
	conn, err := grpc.DialContext(ctx, "localhost:18991",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		))
	if err != nil {
		return err
	}
	defer conn.Close()

	logger.Info("[client] connected")

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
				logger.Debug("[client] [recv] received EOF")
				dataErr.Store(err)
				cancel(err)
				return
			}
			if err != nil {
				logger.Debug("[client] [recv] received error", "error", err)
				dataErr.Store(err)
				cancel(err)
				return
			}
			select {
			case <-handlerClient.Context().Done():
				logger.Debug("[client] [recv] context done", "cause", context.Cause(handlerClient.Context()))
				dataErr.Store(context.Cause(handlerClient.Context()))
				cancel(context.Cause(handlerClient.Context()))
				return
			case dataChan <- data:
			}
		}
	}()

	logger.Info("[client] sending", "data", "test1")
	err = handlerClient.Send(&Data{Data: "test1"})
	if err != nil {
		logger.Error(err.Error())
	}

	for {
		select {
		case <-ctx.Done():
			logger.Debug("[client] context done", "cause", context.Cause(ctx))
			return context.Cause(ctx)
		case data, ok := <-dataChan:
			if !ok {
				logger.Debug("[client] data chan closed", "cause", dataErr.Load())
				derr := dataErr.Load()
				if derr != nil {
					return derr.(error)
				}
				return nil
			}
			logger.Debug("[client] received data", "data", data.Data)
			// case <-time.After(3 * time.Second):
			// 	_ = handlerClient.CloseSend()
			// 	cancel(errors.New("client self-cancel"))
		}
	}
}
