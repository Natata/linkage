package linkage

import (
	"context"
	"linkage/proto/job"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// DialInfo struct is the info for dial to remote service
type DialInfo struct {
	ConnCode   Code
	Addr       Addr
	Opts       []grpc.DialOption
	MaxAttempt int
}

// Client response for build the connection to remote linkage
// and returns the job when user ask it
type Client struct {
	conn   *grpc.ClientConn
	stream job.Service_AskClient
	info   *DialInfo
}

// InitClient reutrn an Client instance
func InitClient(info *DialInfo) (*Client, error) {
	client := &Client{
		info: info,
	}

	return client, nil
}

// BuildStream to recieve jobs from remote lickage server
func (s *Client) BuildStream() error {
	err := s.dial()
	if err != nil {
		log.Errorf("fail to dial, error: %v", err)
		return err
	}
	log.WithFields(log.Fields{
		"address": s.info.Addr,
	}).Info("dial success")

	// ask the stream for job
	err = s.connect()
	if err != nil {
		log.Errorf("fail to connect, error: %v", err)
		return err
	}
	log.Infof("connect success")
	log.Infof("ready to recieve job")
	return nil
}

func (s *Client) dial() error {
	conn, err := grpc.Dial(s.info.Addr, s.info.Opts...)
	if err != nil {
		return err
	}

	s.conn = conn
	return nil
}

func (s *Client) connect() error {
	client := job.NewServiceClient(s.conn)
	stream, err := client.Ask(context.Background(), &job.Passphrase{
		Code: s.info.ConnCode,
	})
	if err != nil {
		return err
	}
	s.stream = stream
	return nil
}

// Ask recieve the job from server
func (s *Client) Ask() (*Job, error) {
	gj, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}

	return toLinkageJob(gj), nil
}

// Close closes the connection
func (s *Client) Close() {
	err := s.conn.Close()
	if err != nil {
		log.Errorf("conn close fail, error: %v", err)
	} else {
		log.Info("conn closed")
	}
	err = s.stream.CloseSend()
	if err != nil {
		log.Errorf("stream close fail, error: %v", err)
	} else {
		log.Info("stream closed")
	}
}
