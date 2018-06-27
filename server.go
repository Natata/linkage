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
	dailInfo   DialInfo
	at         Addr
	srvOpts    []grpc.ServerOption
	engine     Engine
	client     *Client
	income     chan *Job
	outcome    map[Code][]job.Service_AskServer
	done       chan struct{}
	codeAssert CodeAssert
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

// DialInfo struct is the info for dial to remote service
type DialInfo struct {
	connCode Code
	addr     Addr
	opts     []grpc.DialOption
}

// BuildInfo struct
type BuildInfo struct {
	at         Addr
	engine     Engine
	srvOpts    []grpc.ServerOption
	codeAssert CodeAssert
}

// CodeAssert asserts if code is valid
type CodeAssert = func(code Code) bool

// InitServer init server
func InitServer(info BuildInfo) *Server {
	return &Server{
		at:         info.at,
		srvOpts:    info.srvOpts,
		engine:     info.engine,
		income:     make(chan *Job),
		outcome:    map[Code][]job.Service_AskServer{},
		done:       make(chan struct{}),
		codeAssert: info.codeAssert,
	}
}

// Run runs the server
func (s *Server) Run(dialInfo DialInfo) error {
	// connect remote server to get job
	err := s.connect(dialInfo)
	if err != nil {
		return err
	}

	// start engine
	s.income = make(chan *Job)
	go func() {
		s.engine.Start(s.income)
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

func (s *Server) connect(dialInfo DialInfo) error {
	client, err := InitClient(dialInfo.addr, 5)
	if err != nil {
		return err
	}
	err = client.Dial(dialInfo.connCode, dialInfo.opts...)
	if err != nil {
		return err
	}
	s.client = client

	go func() {
		s.requestJob()
	}()
	return nil
}

func (s *Server) requestJob() {

	// TODO: rate limiter

	for {
		j, err := s.client.Ask()
		if err != nil {
			s.Stop()
			return
		}

		s.income <- j
	}
}

// implement jobServer

// Ask implement jobServiceServer interface
func (s *Server) Ask(pass *job.Passphrase, stream job.Service_AskServer) error {
	if !s.codeAssert(pass.GetCode()) {
		return fmt.Errorf("wrong passcode %v", pass.GetCode())
	}

	outcome := make(chan *Job)

	err := s.engine.Register(outcome)
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
	s.client.Close()
	close(s.income)
	err := s.engine.Stop()
	close(s.done)

	return err
}
