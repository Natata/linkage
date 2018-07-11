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

// Waiting define the rule of wait time between each retry
type Waiting func(attempt int, maxAttempt int)

// DialInfo struct is the info for dial to remote service
type DialInfo struct {
	connCode   Code
	addr       Addr
	opts       []grpc.DialOption
	maxAttempt int
}

// Client response for build the connection to remote linkage
// and returns the job when user ask it
type Client struct {
	conn    *grpc.ClientConn
	stream  job.Service_AskClient
	info    *DialInfo
	waiting Waiting
}

// InitClient reutrn an Client instance
func InitClient(info *DialInfo, w Waiting) (*Client, error) {
	if w == nil {
		w = waitToRetry
	}
	client := &Client{
		info:    info,
		waiting: w,
	}

	return client, nil
}

// BuildStream to recieve jobs from remote lickage server
func (s *Client) BuildStream() error {
	err := s.dial()
	if err != nil {
		log.Error("fail to dial")
		return err
	}
	log.WithFields(log.Fields{
		"address": s.info.addr,
	}).Info("dial success")

	// ask the stream for job
	err = s.connect()
	if err != nil {
		log.Error("fail to connect")
		return err
	}
	log.Infof("connect success")
	log.Infof("ready to recieve job")
	return nil
}

func (s *Client) dial() error {
	conn, err := grpc.Dial(s.info.addr, s.info.opts...)
	if err != nil {
		return err
	}

	s.conn = conn
	return nil
}

func (s *Client) connect() error {
	client := job.NewServiceClient(s.conn)
	stream, err := client.Ask(context.Background(), &job.Passphrase{
		Code: s.info.connCode,
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

		return nil, err
	}

	return toLinkageJob(gj), nil
}

// Close closes the connection
func (s *Client) Close() {
	s.conn.Close()
}

func (s *Client) retry() (*job.Job, error) {
	for attempt := 0; attempt < s.info.maxAttempt; attempt++ {
		gj, err := s.stream.Recv()
		if err == nil {
			return gj, nil
		}

		if err == io.EOF {
			return nil, err
		}

		s.waiting(attempt, s.info.maxAttempt)
	}

	return nil, status.New(codes.Unavailable, fmt.Sprintf("try %v times, stream is unavailable", s.info.maxAttempt)).Err()
}

func waitToRetry(times int, maxRetry int) {
	long := time.Duration(math.Pow(2, float64(times)))
	time.Sleep(long * time.Second)
}
