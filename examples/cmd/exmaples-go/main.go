package main

import (
	"context"
	"log"
	"net/http"

	"connectrpc.com/connect"
	elizav1 "github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1"
	"github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1/v1connect"
)

func main() {
	client := v1connect.NewElizaServiceClient(
		http.DefaultClient,
		"http://localhost:8000/",
	)
	req := connect.NewRequest(&elizav1.SayRequest{
		Sentence: "Hi",
	})
	req.Header().Set("Some-Header", "hello from connect")
	res, err := client.Say(context.Background(), req)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(res.Msg)
	log.Println(res.Header())
}
