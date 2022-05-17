package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opt := badger.DefaultOptions
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath
	db, err := badger.Open(opt)
	if err != nil {
		return nil
	}
	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{txn: s.db.NewTransaction(true)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	defer txn.Commit()
	for _, kv := range batch {
		if _, ok := kv.Data.(storage.Delete); ok{
			if err := txn.Delete(engine_util.KeyWithCF(kv.Cf(), kv.Key())); err != nil{
				return err
			}
		}else {
			err := txn.Set(engine_util.KeyWithCF(kv.Cf(), kv.Key()), kv.Value())
			if err != nil{
				return err
			}
		}
	}
	return nil
}

type StandAloneReader struct {
	txn *badger.Txn
}


func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error)  {
	res, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound{
		return nil, nil
	}
	return res, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator  {
	return engine_util.NewCFIterator(cf, r.txn)

}
func (r *StandAloneReader) Close()  {
	r.txn.Commit()
}