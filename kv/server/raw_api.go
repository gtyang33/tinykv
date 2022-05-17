package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err  := server.storage.Reader(req.Context)
	if err != nil{
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	res := &kvrpcpb.RawGetResponse{
		Value:                value,
	}
	if err != nil{
		res.Error = err.Error()
	}
	if len(value) == 0 {
		res.NotFound = true
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	modify := storage.Modify{
		Data: put,
	}
	err := server.storage.Write(req.Context,[]storage.Modify{modify})
	resp := &kvrpcpb.RawPutResponse{
	}
	if err != nil{
		resp.Error = err.Error()
	}
	// Hint: Consider using Storage.Modify to store data to be modified
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{
		Data: delete,
	}
	err := server.storage.Write(req.Context,[]storage.Modify{modify})
	resp := &kvrpcpb.RawDeleteResponse{
	}
	if err != nil{
		resp.Error = err.Error()
	}
	// Hint: Consider using Storage.Modify to store data to be modified
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil{
		return nil, err
	}

	itr:= reader.IterCF(req.GetCf())
	itr.Seek(req.StartKey)
	var kvs []*kvrpcpb.KvPair
	var i uint32
	for itr.Valid(){
		if i >= req.Limit{
			break
		}
		v, err := itr.Item().Value()
		if err != nil{
			return nil, err
		}
		kv := &kvrpcpb.KvPair{
			Key:                  itr.Item().Key(),
			Value:                v,
		}
		kvs = append(kvs, kv)
		itr.Next()
		i++
	}
	itr.Close()
	return &kvrpcpb.RawScanResponse{
		Kvs:    kvs,
	}, nil

}
