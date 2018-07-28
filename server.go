package linkage

import (
	"linkage/proto/job"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implement JobServiceServer and use JobServiceClient
// to recieve job and accept stream request
type Server struct {
	cfg   *ServerConfig
	close Done
	wg    sync.WaitGroup
}

// Result struct
type Result struct {
	Code int
	Msg  string
}

// ServerConfig struct
type ServerConfig struct {
	Addr       Addr
	Engine     Engine
	SrvOpts    []grpc.ServerOption
	CodeAssert CodeAssert
}

// CodeAssert asserts if code is valid
type CodeAssert = func(code Code) bool

// InitServer init server
func InitServer(cfg *ServerConfig) (*Server, error) {
	return &Server{
		cfg:   cfg,
		close: make(Done),
	}, nil
}

// Run runs the server
func (s *Server) Run() error {
	// start this server
	lis, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	ss := s
	gsrv := grpc.NewServer(s.cfg.SrvOpts...)
	job.RegisterServiceServer(gsrv, ss)
	log.Infof("start listening %v", s.cfg.Addr)
	return gsrv.Serve(lis)
}

// implement jobServer

// Ask implement jobServiceServer interface
func (s *Server) Ask(pass *job.Passphrase, stream job.Service_AskServer) error {
	if s.shouldClose() {
		return status.Errorf(code.Abort, "server is closing")
	}

	if !s.info.CodeAssert(pass.GetCode()) {
		return status.Errorf(codes.InvalidArgument, "wrong passcode %v", pass.GetCode())
	}

	s.wg.Add(1)
	defer s.wg.Done()

	sig := make(chan Signal) // TODO: make(chan error)
	var outbound chan *Job
	var err error
	go func() {
		outbound, err = s.cfg.Engine.Register(sig)
		if err != nil {
			log.Errorf("engine register error: %v", err)
			return
		}
	}()

	for j := range outbound {
		gj := toGRPCJob(j)
		err := stream.Send(gj) // TODO: retry?
		if err != nil {
			log.Errorf("err: %v", err)
			sig <- Signal{
				err: err,
			}
			return status.Error(codes.Unavailable, err.Error())
		}

		if s.shouldClose() {
			log.Infof("server closing")
			close(sig)
			break
		}
	}

	// clear all left jobs
	wait := time.After(2 * time.Second)
	for {
		select {
		case j, ok := <-outbound:
			if !ok {
				return status.Error(codes.Unavailable, "service closed")
			}

			gj := toGRPCJob(j)
			err := stream.Send(gj)
			if err != nil {
				log.Errorf("err: %v", err)
				return status.Error(codes.Unavailable, err.Error())
			}
		case <-wait:
			log.Infof("time up")
			return status.Error(codes.Unavailable, "service closed")
		}
	}
}

func (s *Server) shouldClose() bool {
	select {
	case <-s.close:
		return true
	default:
		return false
	}
}

// Close close the server
func (s *Server) Close() Done {
	close(s.close)

	done := make(Done)
	go func() {
		s.wg.Wait()
		close(done)
	}()

	return done
}
