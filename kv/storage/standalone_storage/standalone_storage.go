package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := "standAlonePath"
	kvPath := filepath.Join(dbPath, "kv")
	os.Mkdir(kvPath, os.ModePerm)
	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, nil, kvPath, "")
	return &StandAloneStorage{engines: engines}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engines.Kv.Close(); err != nil {
		return err
	}
	os.RemoveAll(s.engines.KvPath)
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.engines.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
	}
	return s.engines.WriteKV(wb)
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{txn: txn}
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return NewStandAloneDBIterator(engine_util.NewCFIterator(cf, sr.txn))
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}

type StandAloneDBIterator struct {
	iter *engine_util.BadgerIterator
}

func NewStandAloneDBIterator(iter *engine_util.BadgerIterator) *StandAloneDBIterator {
	return &StandAloneDBIterator{iter: iter}
}

func (it *StandAloneDBIterator) Item() engine_util.DBItem {
	return it.iter.Item()
}

func (it *StandAloneDBIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *StandAloneDBIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(prefix)
}

func (it *StandAloneDBIterator) Close() {
	it.iter.Close()
}

func (it *StandAloneDBIterator) Next() {
	it.iter.Next()
}

func (it *StandAloneDBIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *StandAloneDBIterator) Rewind() {
	it.iter.Rewind()
}
