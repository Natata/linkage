package main

import (
	"fmt"
	"linkage"
	"log"
	"time"
)

func main() {
	addr := ":8080"
	engine := &FactorEngine{}
	codeAssert := func(code linkage.Code) bool {
		return true
	}

	srv, err := linkage.InitLinkage(addr, engine, nil, codeAssert, nil, nil)
	if err != nil {
		panic(err)
	}

	err = srv.Run()
	fmt.Println("error: ", err)
}

// FactorEngine engine
type FactorEngine struct{}

// Register chan
func (s *FactorEngine) Register(sig chan linkage.Signal) (<-chan *linkage.Job, error) {

	ch := make(chan *linkage.Job)

	go func() {
		defer close(ch)
		for {
			time.Sleep(3 * time.Second)
			j := &linkage.Job{
				Payload: fmt.Sprintf("%v", time.Now()),
			}

			select {
			case s, ok := <-sig:
				if !ok {
					log.Printf("sig closed")
					return
				}
				if s.Err != nil {
					log.Printf("err sig: %v", s.Err)
					return
				}
				log.Printf("get sig")
			default:
			}
			ch <- j
			log.Println("send")
		}
	}()

	return ch, nil
}

// Start to recieve
func (s *FactorEngine) Start(ch <-chan *linkage.Job) error {

	// No implement

	return nil
}
