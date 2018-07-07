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

// Client response for build the connection to remote linkage
// and returns the job when user ask it
type Client struct {
	addr     string
	conn     *grpc.ClientConn
	stream   job.Service_AskClient
	maxRetry int
}

// InitClient reutrn an Client instance
func InitClient(addr string, maxRetry int) (*Client, error) {
	client := &Client{
		addr:     addr,
		maxRetry: maxRetry,
	}

	return client, nil
}

// Dial to remote lickage server and preserve the stream
func (s *Client) Dial(code Code, opts ...grpc.DialOption) error {
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

func (s *Client) dial(opts ...grpc.DialOption) error {
	conn, err := grpc.Dial(s.addr, opts...)
	if err != nil {
		return err
	}

	s.conn = conn
	return nil
}

func (s *Client) connect(code Code) error {
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
func (s *Client) Ask() (*Job, error) {
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
func (s *Client) Close() {
	s.conn.Close()
}

func (s *Client) retry() (*job.Job, error) {
	for times := 0; times < s.maxRetry; times++ {
		gj, err := s.stream.Recv()
		if err == nil {
			return gj, nil
		}

		if err == io.EOF {
			return nil, err
		}

		waitToRetry(times, s.maxRetry)
	}

	return nil, status.New(codes.Unavailable, fmt.Sprintf("try %v times, stream is unavailable", s.maxRetry)).Err()
}

func waitToRetry(times int, maxRetry int) {
	long := time.Duration(math.Pow(2, float64(times)))
	time.Sleep(long * time.Second)
}
