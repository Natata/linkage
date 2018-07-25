package linkage

import (
	"fmt"
	"linkage/proto/job"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implement JobServiceServer and use JobServiceClient
// to recieve job and accept stream request
type Server struct {
	info *BuildInfo
	done chan struct{}
}

// Engine handle the request
type Engine interface {
	Register(outcome chan<- *Job, closeSig chan struct{}) error
	Start(income <-chan *Job) error
	Stop() error
}

// Result struct
type Result struct {
	Code int
	Msg  string
}

// BuildInfo struct
type BuildInfo struct {
	Addr       Addr
	Engine     Engine
	SrvOpts    []grpc.ServerOption
	CodeAssert CodeAssert
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
	lis, err := net.Listen("tcp", s.info.Addr)
	if err != nil {
		return err
	}
	ss := s
	gsrv := grpc.NewServer(s.info.SrvOpts...)
	job.RegisterServiceServer(gsrv, ss)
	log.Infof("start listening %v", s.info.Addr)
	return gsrv.Serve(lis)
}

// implement jobServer

// Ask implement jobServiceServer interface
func (s *Server) Ask(pass *job.Passphrase, stream job.Service_AskServer) error {
	if !s.info.CodeAssert(pass.GetCode()) {
		return fmt.Errorf("wrong passcode %v", pass.GetCode())
	}

	outcome := make(chan *Job)
	defer close(outcome)
	closeSig := make(chan struct{})

	go func() {
		err := s.info.Engine.Register(outcome, closeSig)
		if err != nil {
			log.Printf("error: %v", err)
			return
		}
	}()

	for j := range outcome {
		gj := toGRPCJob(j)
		err := stream.Send(gj)
		if err != nil {
			log.Printf("err: %v", err)
			close(closeSig)
			return status.Error(codes.Unavailable, err.Error())
		}

		select {
		case <-s.done:
			return nil
		default:
		}
	}

	return nil
}

// Stop stops the server and engine
func (s *Server) Stop() error {
	close(s.done)
	return nil
}
