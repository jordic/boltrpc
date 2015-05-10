package boltrpc

import (
	"errors"
	"net"
	"net/rpc"

	"github.com/boltdb/bolt"
)

var (
	ErrorBucketNotFound = errors.New("bucket not Found")
	ErrorKeyNotFound    = errors.New("key not Found")
)

type Server struct {
	DB *bolt.DB
}

// Query is a rpc query to server
type Query struct {
	Bucket [][]byte // Stores a nested bucket, each index is a level
	Key    []byte
	Value  []byte
}

func (q *Query) SetBucket(b ...[]byte) {
	q.Bucket = b
}

// Response from rpc server
type Response struct {
	Value []byte
	Error string
}

// Creates a Bucket denoted by Query.Key, if Bucket is not present
// in Key, it is created on Root, instead, is created on
// Bucket path.
func (s *Server) CreateBucket(q *Query, r *Response) error {
	// root Bucket
	if len(q.Bucket) == 0 {
		err := s.DB.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(q.Key)
			return err
		})
		if err != nil {
			r.Error = err.Error()
		}
		return nil
	}

	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := NestedBucket(tx, q.Bucket)
		if b == nil {
			return ErrorBucketNotFound
		}
		_, err := b.CreateBucketIfNotExists(q.Key)
		return err
	})

	if err != nil {
		r.Error = err.Error()
	}
	return nil
}

// Sets a Key Query.k to value. If key doesn't exist,
// Creates it
func (s *Server) SetKey(q *Query, r *Response) error {

	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := NestedBucket(tx, q.Bucket)
		if b == nil {
			return ErrorBucketNotFound
		}
		return b.Put(q.Key, q.Value)
	})
	if err != nil {
		r.Error = err.Error()
	}
	return nil
}

// Returns a Key in a bucket
func (s *Server) GetKey(q *Query, r *Response) error {

	err := s.DB.View(func(tx *bolt.Tx) error {
		b := NestedBucket(tx, q.Bucket)
		if b == nil {
			return ErrorBucketNotFound
		}
		r.Value = b.Get(q.Key)
		if r.Value == nil {
			return ErrorKeyNotFound
		}
		return nil
	})
	if err != nil {
		r.Error = err.Error()
	}
	return nil
}

// Deletes a key
func (s *Server) Delete(q *Query, r *Response) error {
	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := NestedBucket(tx, q.Bucket)
		if b == nil {
			return ErrorBucketNotFound
		}
		b.Delete(q.Key)
		return nil
	})

	if err != nil {
		r.Error = err.Error()
	}
	return nil

}

// Deletes a bucket
func (s *Server) DeleteBucket(q *Query, r *Response) error {
	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := NestedBucket(tx, q.Bucket)
		if b == nil {
			return ErrorBucketNotFound
		}
		return b.DeleteBucket(q.Key)
	})

	if err != nil {
		r.Error = err.Error()
	}
	return nil

}

// Returns a nested bucket
func NestedBucket(tx *bolt.Tx, path [][]byte) *bolt.Bucket {
	b := tx.Bucket(path[0])
	if b == nil {
		return nil
	}
	for _, name := range path[1:] {
		b = b.Bucket(name)
		if b == nil {
			return nil
		}
	}
	return b
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
