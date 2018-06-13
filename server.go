package linkage

import (
	"fmt"
	"linkage/proto/job"
	"net"

	"google.golang.org/grpc"
)

// Addr is address type host:port
type Addr = string

// Server implement JobServiceServer and use JobServiceClient
// to recieve job and accept stream request
type Server struct {
	code    Code
	at      Addr
	srvOpts []grpc.ServerOption
	engine  Engine
	agent   *Agent
	jc      chan *Job
}

// Engine handle the request
type Engine interface {
	Register(stream job.Service_AskServer) (chan Result, error)
	Start(jc chan *Job) error
	Stop() error
}

// Result struct
type Result struct {
	Code int
	Msg  string
}

// InitServer init server
func InitServer(code Code, at Addr, engine Engine, srvOpts []grpc.ServerOption) *Server {
	return &Server{
		code:    code,
		at:      at,
		srvOpts: srvOpts,
		engine:  engine,
	}
}

// Run runs the server
func (s *Server) Run(from Addr, dialOpts []grpc.DialOption) error {
	// connect remote server to get job
	err := s.connect(from, dialOpts)
	if err != nil {
		return err
	}

	// start engine
	s.jc = make(chan *Job)
	go func() {
		s.engine.Start(s.jc)
	}()

	// start this server
	lis, err := net.Listen("tcp", s.at)
	if err != nil {
		return err
	}
	ss := s
	gsrv := grpc.NewServer(s.srvOpts...)
	job.RegisterServiceServer(gsrv, ss)
	return gsrv.Serve(lis)
}

func (s *Server) connect(from Addr, opts []grpc.DialOption) error {
	agent, err := InitAgent(from, 5, s.code, opts...)
	if err != nil {
		return err
	}
	s.agent = agent

	go func() {
		s.requestJob()
	}()
	return nil
}

func (s *Server) requestJob() {

	// TODO: rate limiter

	for {
		j, err := s.agent.Ask()
		if err != nil {
			return
		}

		s.jc <- j
	}
}

// implement jobServer

// Ask implement jobServiceServer interface
func (s *Server) Ask(pass *job.Passphrase, stream job.Service_AskServer) error {
	if pass.Code != s.code {
		return fmt.Errorf("wrong passcode")
	}

	done, err := s.engine.Register(stream)
	if err != nil {
		return err
	}

	result := <-done
	if result.Code != 0 {
		return fmt.Errorf(result.Msg)
	}

	return nil
}

//func (s *Server) Pause()  {}
//func (s *Server) Resume() {}

// Stop stops the server and engine
func (s *Server) Stop() error {
	s.agent.Close()
	close(s.jc)
	return s.engine.Stop()
}
