package main

import (
	"fmt"
	"linkage"

	"google.golang.org/grpc"
)

func main() {
	bi := &linkage.BuildInfo{
		Addr:   ":8081",
		Engine: &RoadEngine{},
		CodeAssert: func(code linkage.Code) bool {
			return true
		},
	}

	di := &linkage.DialInfo{
		ConnCode:   "yo",
		Addr:       ":8080",
		Opts:       []grpc.DialOption{grpc.WithInsecure()},
		MaxAttempt: 2,
	}

	srv, err := linkage.InitLinkage(bi, di, nil)
	if err != nil {
		panic(err)
	}

	err = srv.Run()
	fmt.Println("error: ", err)

}

// RoadEngine engine
type RoadEngine struct {
}

// Register chan
func (s *RoadEngine) Register(ch chan<- *linkage.Job, closeSig chan struct{}) error {

	// No implement

	return nil
}

// Start to recieve
func (s *RoadEngine) Start(ch <-chan *linkage.Job) error {
	for v := range ch {
		fmt.Println("i got: ", v)
	}

	return nil
}

// Stop engine
func (s *RoadEngine) Stop() error {
	return nil
}
