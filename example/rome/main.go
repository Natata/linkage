package main

import (
	"fmt"
	"linkage"
	"log"
	"time"
)

func main() {
	bi := &linkage.BuildInfo{
		Addr:   ":8080",
		Engine: &FactorEngine{},
		CodeAssert: func(code linkage.Code) bool {
			return true
		},
	}

	srv, err := linkage.InitLinkage(bi, nil, nil)
	if err != nil {
		panic(err)
	}

	err = srv.Run()
	fmt.Println("error: ", err)
}

// FactorEngine engine
type FactorEngine struct{}

// Register chan
func (s *FactorEngine) Register(ch chan<- *linkage.Job, closeSig chan struct{}) error {
	for {
		time.Sleep(3 * time.Second)
		j := &linkage.Job{
			Payload: fmt.Sprintf("%v", time.Now()),
		}
		select {
		case <-closeSig:
			log.Printf("close ")
			return nil
		default:
		}
		ch <- j
		log.Println("send")
	}
}

// Start to recieve
func (s *FactorEngine) Start(ch <-chan *linkage.Job) error {

	// No implement

	return nil
}

// Stop engine
func (s *FactorEngine) Stop() error {
	return nil
}
