package boltrpc

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

func tempfile() string {
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func OpenDB(f string) *bolt.DB {

	d, err := bolt.Open(f, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal("DB:", err)
	}
	return d
}

func TestCreateBucket(t *testing.T) {

	file := tempfile()
	db := OpenDB(file)

	s := &Server{
		DB: db,
	}

	q := &Query{
		Key: []byte("a"),
	}

	r := &Response{}
	s.CreateBucket(q, r)

	if r.Error != "" {
		t.Errorf("Error creating bucket at root")
	}

	q.SetBucket([]byte("a"))
	q.Key = []byte("b")
	s.CreateBucket(q, r)

	if r.Error != "" {
		t.Errorf("Error creating nested bucket")
	}

	q.Key = []byte("test")
	q.Value = []byte("works")

	s.SetKey(q, r)
	if r.Error != "" {
		t.Error("Unable to set key")
	}

	s.GetKey(q, r)
	if string(r.Value) != "works" {
		t.Error("Unable to fetch value")
	}

	// Set a key to a non existent bucket.
	q.SetBucket([]byte("test"))
	s.SetKey(q, r)
	if r.Error != ErrorBucketNotFound.Error() {
		t.Error("Not created bucket should return an error")
	}

	r = &Response{}
	// Set a key on a nested bucket
	q.SetBucket([]byte("a"), []byte("b"))
	q.Key = []byte("nested")
	q.Value = []byte("nestedvalue")
	s.SetKey(q, r)

	if r.Error != "" {
		t.Errorf("Setting a key on a nested bucket should not return an error %s", r.Error)
	}

	s.GetKey(q, r)
	if string(r.Value) != "nestedvalue" {
		t.Error("Unable to fetch value")
	}

	r = &Response{}
	q.Key = []byte("asdf")
	s.GetKey(q, r)
	if r.Error != ErrorKeyNotFound.Error() {
		t.Error("Should return error key not found")
	}

	r = &Response{}
	q.Key = []byte("nested")
	s.Delete(q, r)
	if r.Error != "" {
		t.Errorf("Key should be deleted without complaining %s", r.Error)
	}

	s.GetKey(q, r)
	if r.Error != ErrorKeyNotFound.Error() {
		t.Errorf("Should return error key not found %v", r.Error)
	}
	if r.Value != nil {
		t.Error("Key must be nil because is not found")
	}

	// Delete a bucket
	r = &Response{}
	q.SetBucket([]byte("a"))
	q.Key = []byte("b")
	s.DeleteBucket(q, r)

	if r.Error != "" {
		t.Error("Deleting a bucket should return nil error")
	}

	q.Key = []byte("c")
	s.DeleteBucket(q, r)
	if r.Error != "bucket not found" {
		t.Errorf("Cant delete a non existent bucket: %s", r.Error)
	}

	q.SetBucket([]byte("a"), []byte("b"))
	q.Key = []byte("nested")
	q.Value = []byte("nestedvalue")
	s.SetKey(q, r)

	if r.Error != ErrorBucketNotFound.Error() {
		t.Error("Bucket should don't exists")
	}

}

func TestServerCreation(t *testing.T) {
	file := tempfile()
	db := OpenDB(file)

	db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		err = b.Put([]byte("hola"), []byte("mon"))
		return err
	})

	s := &Server{
		DB: db,
	}

	q := &Query{
		Key: []byte("hola"),
	}
	q.SetBucket([]byte("test"))

	r := &Response{}

	s.GetKey(q, r)

	if string(r.Value) != "mon" {
		t.Error("Returned %s", r.Value)
	}

	l, err := NewHTTPListenerRpc(s, ":1234")
	go http.Serve(l, nil)

	client, err := rpc.DialHTTP("tcp", ":1234")
	if err != nil {
		t.Errorf("Client error %s", err)
	}
	r = new(Response)
	err = client.Call("Bolt.GetKey", q, &r)
	if err != nil {
		t.Errorf("Error client call %s", err)
	}

	if string(r.Value) != "mon" {
		t.Error("Returned %s", r.Value)
	}

	q1 := &Query{
		Key:   []byte("a"),
		Value: []byte("b"),
	}

	q1.SetBucket([]byte("test"))
	r = new(Response)
	err = client.Call("Bolt.SetKey", q1, &r)
	if r.Error != "" {
		t.Errorf("Error client call %s", r.Error)
	}

	q1.SetBucket([]byte("a"))
	r = new(Response)
	err = client.Call("Bolt.SetKey", q1, &r)
	if r.Error == "" {
		t.Errorf("Error client call %#v, %#v", r.Error, ErrorBucketNotFound)
	}

	os.Remove(file)

}
