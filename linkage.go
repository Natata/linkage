package linkage

import (
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Linkage is the main service to require jobs
// and send jobs to connected client.
// It also an Engine
type Linkage struct {
	server       *Server
	client       *Client
	engine       Engine
	income       chan *Job
	waiting      Waiting
	closeCh      chan struct{}
	serverDoneCh chan struct{}
}

// InitLinkage init a linkage service
func InitLinkage(addr Addr, engine Engine, srvOpts []grpc.ServerOption, codeAssert CodeAssert, di *DialInfo, w Waiting) (*Linkage, error) {

	// TODO: check parameter

	if w == nil {
		log.Info("use defult wait to retry mechanism")
		start := 1
		grow := 2
		maxAttempt := 3
		w = WaitFactory(start, grow, maxAttempt)
	}

	l := &Linkage{
		server:  nil,
		client:  nil,
		engine:  engine,
		income:  make(chan *Job),
		waiting: w,
		closeCh: make(chan struct{}),
	}

	if di != nil {
		log.Infof("initial client")
		cli, err := InitClient(di)
		if err != nil {
			log.Errorf("fail to initial client")
			return nil, err
		}
		log.Infof("initial client success")
		l.client = cli
	}

	srvCfg := &ServerConfig{
		Addr:       addr,
		Engine:     l,
		SrvOpts:    srvOpts,
		CodeAssert: codeAssert,
	}
	srv, err := InitServer(srvCfg)
	if err != nil {
		return nil, err
	}
	log.Printf("init server")
	l.server = srv
	l.engine = engine
	return l, nil
}

// Run start to run linkage service
func (s *Linkage) Run() error {
	// start client
	if s.client != nil {
		err := s.client.BuildStream()
		if err != nil {
			st := status.Convert(err)
			log.WithFields(log.Fields{
				"error_code":    st.Code(),
				"error_message": st.Message(),
			}).Error("fail to build stream")
			return err
		}
		go s.askJobRoutine()
	}

	// start engine
	go func() {
		err := s.engine.Start(s.income)
		if err != nil {
			s.Stop()
		}
	}()

	// start server
	go func() {
		err := s.server.Run()
		if err != nil {
			s.Stop()
		}
	}()

	<-s.closeCh
	return nil
}

// Register interface
func (s *Linkage) Register(sig chan Signal) (<-chan *Job, error) {
	return s.engine.Register(sig)
}

// Start interface
func (s *Linkage) Start(<-chan *Job) error {
	return nil
}

// Stop interface
func (s *Linkage) Stop() error {
	s.client.Close()

	done := s.server.Close()
	timeThreshold := time.After(2 * time.Minute)
	select {
	case <-done:
		log.Infof("server close gracefully")
	case <-timeThreshold:
		log.Infof("server close at timeup")
	}

	close(s.closeCh)
	return nil
}

func (s *Linkage) askJobRoutine() {

	// TODO: rate limit

	for {
		err := s.askJob()
		if err != nil {
			log.Errorf("ask job fail, error: %v", err)
			log.Info("stop linkage")
			s.Stop()
			return
		}
	}
}

func (s *Linkage) askJob() error {
	j, err := s.retry()
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}

		st := status.Convert(err)
		log.WithFields(log.Fields{
			"error_code":    st.Code(),
			"error_message": st.Message(),
			"address":       s.client.info.Addr,
		}).Error("recieve fail, close connect")
		return st.Err()
	}

	s.income <- j
	return nil
}

func (s *Linkage) retry() (*Job, error) {
	for {
		gj, err := s.client.Ask()
		if err == nil {
			return gj, nil
		}

		if err == io.EOF {
			return nil, err
		}

		log.Errorf("recieve fail, error: %v", err)
		log.Info("wait to retry")
		err = s.waiting()
		if err != nil {
			return nil, err
		}
	}
}
