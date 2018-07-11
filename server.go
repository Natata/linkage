package linkage

import (
	"fmt"
	"linkage/proto/job"
	"net"

	"google.golang.org/grpc"
)

// Server implement JobServiceServer and use JobServiceClient
// to recieve job and accept stream request
type Server struct {
	info *BuildInfo
	done chan struct{}
}

// Engine handle the request
type Engine interface {
	Register(chan<- *Job) error
	Start(<-chan *Job) error
	Stop() error
}

// Result struct
type Result struct {
	Code int
	Msg  string
}

// BuildInfo struct
type BuildInfo struct {
	addr       Addr
	engine     Engine
	srvOpts    []grpc.ServerOption
	codeAssert CodeAssert
}

// CodeAssert asserts if code is valid
type CodeAssert = func(code Code) bool

// InitServer init server
func InitServer(info *BuildInfo) (*Server, error) {
	return &Server{
		info: info,
		done: make(chan struct{}),
	}, nil
}

// Run runs the server
func (s *Server) Run() error {
	// start this server
	lis, err := net.Listen("tcp", s.info.addr)
	if err != nil {
		return err
	}
	ss := s
	gsrv := grpc.NewServer(s.info.srvOpts...)
	job.RegisterServiceServer(gsrv, ss)
	return gsrv.Serve(lis)
}

// implement jobServer

// Ask implement jobServiceServer interface
func (s *Server) Ask(pass *job.Passphrase, stream job.Service_AskServer) error {
	if !s.info.codeAssert(pass.GetCode()) {
		return fmt.Errorf("wrong passcode %v", pass.GetCode())
	}

	outcome := make(chan *Job)
	defer close(outcome)

	err := s.info.engine.Register(outcome)
	if err != nil {
		return err
	}

	for j := range outcome {
		gj := toGRPCJob(j)
		err := stream.Send(gj)
		if err != nil {
			return err
		}

		select {
		case <-s.done:
			return nil
		default:
		}
	}

	return nil
}

//func (s *Server) Pause()  {}
//func (s *Server) Resume() {}

// Stop stops the server and engine
func (s *Server) Stop() error {
	close(s.done)

	return nil
}
