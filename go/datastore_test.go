package datastore

import (
	"code.google.com/p/goprotobuf/proto"
	"net/http"
	"testing"
	"github.com/GoogleCloudPlatform/google-cloud-datastore/go"
	pb "github.com/GoogleCloudPlatform/google-cloud-datastore/go/proto/datastore_v1"
	"strings"
	"io/ioutil"
)

type FakeTransport struct {
	Request  *http.Request
	Response *http.Response
}

func (t *FakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.Request = req
	return t.Response, nil
}

type callType func(datasetId string, req proto.Message) (proto.Message, error)

func testDo(t *testing.T, op string, req proto.Message, c *datastore.Connection, call callType) {
	ft := FakeTransport{
		Response: &http.Response{Body: ioutil.NopCloser(strings.NewReader(""))},
	}
	c.Transport = &ft

	_, err := call("dataset", req)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if ft.Request.Method != "POST" {
		t.Error("Bad HTTP method (expecte POST) in request:", ft.Request)
	}
	if !strings.HasSuffix(ft.Request.URL.Path, op) {
		t.Error("Bad path (not a lookup) in request:", ft.Request)
	}
	if ft.Request.Header.Get("Content-Type") != "application/x-protobuf" {
		t.Error("Bad Content-Type (expected application/x-protobuf) in request:", ft.Request)
	}
}

func TestAllocateIds(t *testing.T) {
	c := datastore.Connection{}
	call := func(datasetId string, req proto.Message) (proto.Message, error) {
		return c.AllocateIds(datasetId, req.(*pb.AllocateIdsRequest))
	}
	testDo(t, "allocateIds", &pb.AllocateIdsRequest{}, &c, call)
}

func TestBeginTransaction(t *testing.T) {
	c := datastore.Connection{}
	call := func(datasetId string, req proto.Message) (proto.Message, error) {
		return c.BeginTransaction(datasetId, req.(*pb.BeginTransactionRequest))
	}
	testDo(t, "beginTransaction", &pb.BeginTransactionRequest{}, &c, call)
}

func TestCommit(t *testing.T) {
	c := datastore.Connection{}
	call := func(datasetId string, req proto.Message) (proto.Message, error) {
		return c.Commit(datasetId, req.(*pb.CommitRequest))
	}
	testDo(t, "commit", &pb.CommitRequest{}, &c, call)
}

func TestLookup(t *testing.T) {
	c := datastore.Connection{}
	call := func(datasetId string, req proto.Message) (proto.Message, error) {
		return c.Lookup(datasetId, req.(*pb.LookupRequest))
	}
	testDo(t, "lookup", &pb.LookupRequest{}, &c, call)
}

func TestRollback(t *testing.T) {
	c := datastore.Connection{}
	call := func(datasetId string, req proto.Message) (proto.Message, error) {
		return c.Rollback(datasetId, req.(*pb.RollbackRequest))
	}
	testDo(t, "rollback", &pb.RollbackRequest{Transaction: []byte{}}, &c, call)
}

func TestRunQuery(t *testing.T) {
	c := datastore.Connection{}
	call := func(datasetId string, req proto.Message) (proto.Message, error) {
		return c.RunQuery(datasetId, req.(*pb.RunQueryRequest))
	}
	testDo(t, "runQuery", &pb.RunQueryRequest{}, &c, call)
}
