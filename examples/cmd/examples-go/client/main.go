package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	"connectrpc.com/connect"
	elizav1 "github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1"
	"github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1/v1connect"
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
	client := v1connect.NewElizaServiceClient(http.DefaultClient, "http://localhost:8080/")

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
			return err
		}

	case bidirectionalStreaming:
		// TODO(tsubakiky): no-op yet
	}

	return nil
}
