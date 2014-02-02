package datastore

import (
	"code.google.com/p/goprotobuf/proto"
	pb "github.com/GoogleCloudPlatform/google-cloud-datastore/go/proto/datastore_v1"
	"io/ioutil"
	"net/http"
	"bytes"
	"strconv"
)

type Connection struct {
	Transport http.RoundTripper
}

const BasePath = "https://www.googleapis.com/datastore/v1beta2/datasets/"
const (
	DatastoreScope = "https://www.googleapis.com/auth/datastore"
	UserinfoEmailScope = "https://www.googleapis.com/auth/userinfo.email"
)

func (c *Connection) AllocateIds(datasetId string, req *pb.AllocateIdsRequest) (*pb.AllocateIdsResponse, error) {
	res := &pb.AllocateIdsResponse{}
	return res, c.do(datasetId, "allocateIds", req, res)
}

func (c *Connection) BeginTransaction(datasetId string, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	res := &pb.BeginTransactionResponse{}
	return res, c.do(datasetId, "beginTransaction", req, res)
}

func (c *Connection) Commit(datasetId string, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	res := &pb.CommitResponse{}
	return res, c.do(datasetId, "commit", req, res)
}

func (c *Connection) Lookup(datasetId string, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	res := &pb.LookupResponse{}
	return res, c.do(datasetId, "lookup", req, res)
}

func (c *Connection) Rollback(datasetId string, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	res := &pb.RollbackResponse{}
	return res, c.do(datasetId, "rollback", req, res)
}

func (c *Connection) RunQuery(datasetId string, req *pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
	res := &pb.RunQueryResponse{}
	return res, c.do(datasetId, "runQuery", req, res)
}

func (c *Connection) do(datasetId string, op string, req proto.Message, res proto.Message) error {
	s, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(s)

	httpReq, err := http.NewRequest("POST", BasePath + datasetId + "/" + op, buf)
	if err != nil {
		return err
	}

	httpReq.URL.Opaque = "//" + httpReq.URL.Host + httpReq.URL.Path
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Length", strconv.Itoa(buf.Len()))

	httpRes, err := c.Transport.RoundTrip(httpReq)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(httpRes.Body)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, res)
}
