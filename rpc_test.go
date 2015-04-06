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
		Bucket: []byte("test"),
		Key:    []byte("hola"),
	}

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
		Bucket: []byte("test"),
		Key:    []byte("a"),
		Value:  []byte("b"),	
	}
	r = new(Response)
	err = client.Call("Bolt.SetKey", q1, &r)
	if err != nil {
		t.Errorf("Error client call %s", err)
	}

	q1.Bucket = []byte("a")
	err = client.Call("Bolt.SetKey", q1, &r)
	if err == nil {
		t.Errorf("Error client call %#v, %#v", err, ErrorBucketNotFound)
	}
	
	
	os.Remove(file)

}