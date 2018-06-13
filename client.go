package linkage

import (
	"context"
	"fmt"
	"io"
	"linkage/proto/job"
	"math"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Agent response for build the connection
// and returns the job when user ask it
type Agent struct {
	addr       string
	conn       *grpc.ClientConn
	stream     job.Service_AskClient
	retryLimit int
}

// InitAgent reutrn an Agent instance
func InitAgent(addr string, retryLimit int, code Code, opts ...grpc.DialOption) (*Agent, error) {
	agent := &Agent{
		addr:       addr,
		retryLimit: retryLimit,
	}

	// prepare for recieving data
	err := agent.start(code, opts...)
	if err != nil {
		return nil, err
	}

	return agent, nil
}

// start build the connection and preserve the stream
func (s *Agent) start(code Code, opts ...grpc.DialOption) error {
	err := s.dial(opts...)
	if err != nil {
		st := status.Convert(err)
		log.WithFields(log.Fields{
			"error_code":    st.Code(),
			"error_message": st.Message(),
		}).Error("fail to dial")
		return err
	}
	log.WithFields(log.Fields{
		"address": s.addr,
	}).Info("dial success")

	// ask the stream for job
	err = s.connect(code)
	if err != nil {
		st := status.Convert(err)
		log.WithFields(log.Fields{
			"code":          code,
			"error_code":    st.Code(),
			"error_message": st.Message(),
		}).Errorf("fail to connect")
		return err
	}
	log.Infof("connect success")
	log.Infof("ready to recieve job")
	return nil
}

func (s *Agent) dial(opts ...grpc.DialOption) error {
	conn, err := grpc.Dial(s.addr, opts...)
	if err != nil {
		return err
	}

	s.conn = conn
	return nil
}

func (s *Agent) connect(code Code) error {
	client := job.NewServiceClient(s.conn)
	stream, err := client.Ask(context.Background(), &job.Passphrase{
		Code: code,
	})
	if err != nil {
		return err
	}
	s.stream = stream

	return nil
}

// Ask recieve the job from server
func (s *Agent) Ask() (*Job, error) {
	gj, err := s.retry()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}

		st := status.Convert(err)
		log.WithFields(log.Fields{
			"error_code":    st.Code(),
			"error_message": st.Message(),
			"address":       s.addr,
		}).Error("recieve fail, close connect")
		s.Close()
		return nil, err
	}

	return toLinkageJob(gj), nil
}

// Close closes the connection
func (s *Agent) Close() {
	s.conn.Close()
}

func (s *Agent) retry() (*job.Job, error) {
	for times := 0; times < s.retryLimit; times++ {
		gj, err := s.stream.Recv()
		if err == nil {
			return gj, nil
		}

		if err == io.EOF {
			return nil, err
		}
		long := time.Duration(math.Pow(2, float64(times)))
		time.Sleep(long * time.Second)
	}

	return nil, status.New(codes.Unavailable, fmt.Sprintf("try %v times, stream is unavailable", s.retryLimit)).Err()
}
