package main

import (
	"fmt"
	"linkage"

	"google.golang.org/grpc"
)

func main() {
	addr := ":8081"
	engine := &RoadEngine{}
	codeAssert := func(code linkage.Code) bool {
		return true
	}

	di := &linkage.DialInfo{
		ConnCode:   "yo",
		Addr:       ":8080",
		Opts:       []grpc.DialOption{grpc.WithInsecure()},
		MaxAttempt: 2,
	}

	srv, err := linkage.InitLinkage(addr, engine, nil, codeAssert, di, nil)
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
func (s *RoadEngine) Register(sig chan linkage.Signal) (<-chan *linkage.Job, error) {

	// No implement

	return make(chan *linkage.Job), nil
}

// Start to recieve
func (s *RoadEngine) Start(ch <-chan *linkage.Job) error {
	for v := range ch {
		fmt.Println("i got: ", v)
	}

	return nil
}
