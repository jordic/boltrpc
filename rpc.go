package boltrpc

import (
	"errors"
	"net"
	"net/rpc"

	"github.com/boltdb/bolt"
)

var (
	ErrorBucketNotFound = errors.New("Bucket not Found")
	)


type Server struct {
	DB *bolt.DB
}

type Query struct {
	Bucket []byte
	Key    []byte
	Value  []byte
}

type Response struct {
	Value []byte
}

func (s *Server) GetKey(q *Query, r *Response) error {

	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(q.Bucket)
		if b == nil {
			return nil
		}
		r.Value = b.Get(q.Key)
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

func (s *Server) SetKey(q *Query, r *Response) error {
	
	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(q.Bucket)
		if b == nil {
			return ErrorBucketNotFound
		}
		return b.Put(q.Key, q.Value)
	})
	return err
}

func NewHTTPListenerRpc(s *Server, addr string) (net.Listener, error) {

	rpc.RegisterName("Bolt", s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return nil, e
	}
	return l, nil
}
