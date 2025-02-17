package main

import (
	"context"
	"log"
	"net/http"

	"connectrpc.com/connect"
	elizav1 "github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1"
	"github.com/gaudiy/connect-python/examples/proto/connectrpc/eliza/v1/v1connect"
)

// func main() {
// 	client := v1connect.NewElizaServiceClient(
// 		http.DefaultClient,
// 		"http://localhost:8080/",
// 	)
// 	req := connect.NewRequest(&elizav1.SayRequest{
// 		Sentence: "Hi",
// 	})
// 	req.Header().Set("Some-Header", "hello from connect")
// 	res, err := client.Say(context.Background(), req)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	log.Println(res.Msg)
// 	log.Println(res.Header())
// }

// func main() {
// 	client := v1connect.NewElizaServiceClient(
// 		http.DefaultClient,
// 		"http://localhost:8080/",
// 	)

// 	stream := client.IntroduceClient(context.Background())
// 	for i := 0; i < 5; i++ {
// 		err := stream.Send(&elizav1.IntroduceRequest{
// 			Name: "Alice",
// 		})
// 		if err != nil {
// 			log.Fatalln(err)
// 			break
// 		}
// 	}

// 	res, err := stream.CloseAndReceive()
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	log.Println(res.Msg)
// }

func main() {
	client := v1connect.NewElizaServiceClient(
		http.DefaultClient,
		"http://localhost:8080/",
	)

	request := connect.NewRequest(&elizav1.IntroduceRequest{
		Name: "Alice",
	})
	stream, err := client.IntroduceServer(context.Background(), request)
	if err != nil {
		log.Fatalln(err)
	}

	number := int64(1)
	for ; stream.Receive(); number++ {
		log.Printf("Received message %d: %s", number, stream.Msg().Sentence)
	}
}
