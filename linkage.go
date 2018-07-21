package linkage

import (
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
)

// Linkage is the main service to require jobs
// and send jobs to connected client.
// It also an Engine
type Linkage struct {
	server *Server
	client *Client
	engine Engine
	income chan *Job
}

// InitLinkage init a linkage service
func InitLinkage(bi *BuildInfo, di *DialInfo, w Waiting) (*Linkage, error) {
	if bi == nil {
		return nil, fmt.Errorf("should have build info")
	}

	l := &Linkage{
		server: nil,
		client: nil,
		income: make(chan *Job),
	}

	if di != nil {
		cli, err := InitClient(di, w)
		if err != nil {
			return nil, err
		}
		l.client = cli
	}

	// NOTE: use linkage as the engine of server
	en := bi.Engine
	bi.Engine = l
	srv, err := InitServer(bi)
	if err != nil {
		return nil, err
	}
	log.Printf("init server")
	l.server = srv
	l.engine = en
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
	go s.engine.Start(s.income)

	// start server
	return s.server.Run()
}

// Register interface
func (s *Linkage) Register(outcome chan<- *Job, closeSig chan struct{}) error {
	return s.engine.Register(outcome, closeSig)
}

// Start interface
func (s *Linkage) Start(<-chan *Job) error {
	return nil
}

// Stop interface
func (s *Linkage) Stop() error {
	close(s.income)
	s.client.Close()
	s.server.Stop()
	return nil
}

func (s *Linkage) askJobRoutine() {
	for {
		err := s.askJob()
		if err != nil {
			s.Stop()
			s.client.Close()
			return
		}
	}
}

func (s *Linkage) askJob() error {
	j, err := s.client.Ask()
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
		return err
	}

	s.income <- j
	return nil
}
