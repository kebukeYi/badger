package badger

import (
	"fmt"
	"github.com/dgraph-io/badger/v4/y"
	"testing"
)

func TestTxn_ReadTs(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		// readTs           commitTs    send
		// 0                1          channel
		db.NewTransaction(true).Commit() //内存索引立马更改, 但是channel通道处理缓慢;

		// 1                1
		db.NewTransaction(true) // 申请的readTs=1,但是上一个 readTs=0还没处理完,因此会阻塞;
		// 1                1
		db.NewTransaction(true) // 当第一个提交完毕后,此后的读取都将无堵塞,因为没有涉及到提交;

		// 1                2
		db.NewTransaction(true).Commit() //内存索引立马更改, 但是channel通道处理缓慢;

		// 2                2
		db.NewTransaction(true) // 申请的readTs=2,但是上一个 readTs=1还没处理完,因此会阻塞;
		db.NewTransaction(true).Commit()
		db.NewTransaction(true)
	})
}
func TestTxns_Commit(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)

		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("key=%d", i))
			v := []byte(fmt.Sprintf("oneval=%d", i))
			txn.SetEntry(NewEntry(k, v))
		}

		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("key=%d", i))
			v := []byte(fmt.Sprintf("twoval=%d", i))
			txn.SetEntry(NewEntry(k, v))
		}

		for i := 0; i < 10; i++ {
			k := []byte(fmt.Sprintf("key=%d", i))
			v := []byte(fmt.Sprintf("threeval=%d", i))
			txn.SetEntry(NewEntry(k, v))
		}

		item, err := txn.Get([]byte("key=8"))
		fmt.Sprintf("oneitme: %v; err:%s", item, err)

		item, err = txn.Get([]byte("threekey=8"))
		fmt.Sprintf("threeitme: %v; err:%s", item, err)

		// _ = txn.CommitAt(100, nil)
		txn.Commit() // 提交事务后, 当前事务就失效;

		item, err = txn.Get([]byte("key=8"))
		fmt.Sprintf("oneitme: %v; err:%s", item, err)

		item, err = txn.Get([]byte("threekey=8"))
		fmt.Sprintf("threeitme: %v; err:%s", item, err)
	})
}

func TestKeyVersion(t *testing.T) {
	key := []byte("aaa")
	key1 := y.KeyWithTs(key, 1)  // version:1
	key2 := y.KeyWithTs(key, 10) // version: 10
	compareKeys := y.CompareKeys(key1, key2)
	fmt.Sprintf("compareKeys: %d", compareKeys)
}
