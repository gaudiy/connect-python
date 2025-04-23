package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"connectrpc.com/connect"
	elizav1 "github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1"
	"github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1/v1connect"
	"golang.org/x/net/http2"
)

type mode int

const (
	modeClient mode = iota + 1
	// for future use?
	modeServer
)

type rpc int

const (
	unary rpc = iota + 1
	serverStreaming
	clientStreaming
	bidirectionalStreaming
)

var (
	runMode mode
	rpcType rpc
)

func init() {
	// mode
	clientMode := flag.Bool("c", false, "run client mode")
	serverMode := flag.Bool("s", false, "run server mode")
	// rpc
	unaryRPC := flag.Bool("u", false, "send unary RPC")
	serverStreamingRPC := flag.Bool("ss", false, "send unary RPC")
	clientStreamingRPC := flag.Bool("cs", false, "send unary RPC")
	bidiRPC := flag.Bool("bidi", false, "send unary RPC")

	flag.Parse()

	switch {
	case *clientMode:
		runMode = modeClient
	case *serverMode:
		runMode = modeServer
	default: // throw
		if !*clientMode && !*serverMode {
			log.Fatal("either clientMode or serverMode must be enabled. [-c, -s]")
		}
		if *clientMode && *serverMode {
			log.Fatal("neither clientMode nor serverMode can't be enabled")
		}
		panic("unreachable")
	}

	switch {
	case *unaryRPC:
		rpcType = unary
	case *serverStreamingRPC:
		rpcType = serverStreaming
	case *clientStreamingRPC:
		rpcType = clientStreaming
	case *bidiRPC:
		rpcType = bidirectionalStreaming
	default: // throw
		log.Fatal("please enable any rpc type. [-u, -ss, -cs, -bidi]")
	}
}

func asError(err error) (*connect.Error, bool) {
	var connectErr *connect.Error
	ok := errors.As(err, &connectErr)
	return connectErr, ok
}

func main() {
	switch runMode {
	case modeClient:
		if err := runClient(); err != nil {
			log.Fatal(err)
		}
	case modeServer:
		// TODO(tsubakiky): not implemented yet
	}
}

func runClient() error {
	// Create an HTTP/2 transport that allows unencrypted HTTP/2
	transport := &http2.Transport{
		AllowHTTP: true,
		// Pretend we are dialing TLS but we are not
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			return net.Dial(network, addr)
		},
	}

	// Create an HTTP client with the HTTP/2 transport
	httpClient := &http.Client{
		Transport: transport,
	}

	// Create the ElizaService client with the HTTP/2 client
	client := v1connect.NewElizaServiceClient(httpClient, "http://localhost:8080/", connect.WithGRPC())

	ctx := context.Background()
	switch rpcType {
	case unary:
		req := connect.NewRequest(&elizav1.SayRequest{
			Sentence: "Hi",
		})
		req.Header().Set("Some-Header", "hello from connect")
		res, err := client.Say(ctx, req)
		if err != nil {
			return err
		}
		fmt.Printf("res.Msg: %v\n", res.Msg)
		fmt.Printf("res.Header(): %v\n", res.Header())

	case clientStreaming:
		stream := client.IntroduceClient(ctx)
		for range 5 {
			err := stream.Send(&elizav1.IntroduceRequest{
				Name: "Alice",
			})
			if err != nil {
				return err
			}
		}
		res, err := stream.CloseAndReceive()
		if err != nil {
			if connectErr, ok := asError(err); ok {
				fmt.Printf("Error: %s\n", connectErr.Message())
				fmt.Printf("Error code: %d\n", connectErr.Code())
				fmt.Printf("Error details: %v\n", connectErr.Details())
				fmt.Printf("Error metadata: %v\n", connectErr.Meta())
			}
			return err
		}
		fmt.Printf("res.Msg: %v\n", res.Msg)

	case serverStreaming:
		request := connect.NewRequest(&elizav1.IntroduceRequest{
			Name: "Alice",
		})
		stream, err := client.IntroduceServer(ctx, request)
		if err != nil {
			return err
		}
		defer stream.Close()

		for number := int64(1); stream.Receive(); number++ {
			fmt.Printf("Received message %d: %s\n", number, stream.Msg().Sentence)
		}

		if err := stream.Err(); err != nil {
			if connectErr, ok := asError(err); ok {
				fmt.Printf("Error: %s\n", connectErr.Message())
				fmt.Printf("Error code: %d\n", connectErr.Code())
				fmt.Printf("Error details: %v\n", connectErr.Details())
				fmt.Printf("Error metadata: %v\n", connectErr.Meta())
			}
			return err
		}

	case bidirectionalStreaming:
		// TODO(tsubakiky): no-op yet
	}

	return nil
}
