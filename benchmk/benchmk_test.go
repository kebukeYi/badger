package benchmk

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"testing"
	"time"
)

//var benchMarkDir = "F:\\ProjectsData\\golang\\TrainBadger\\test\\benchmk"

var benchMarkDir = "/usr/projects/golangprojects/badger/benchmk"

var benchMarkOpt = badger.Options{
	Dir:      benchMarkDir,
	ValueDir: benchMarkDir,

	MemTableSize:        10 << 20, //  64 << 20
	BaseTableSize:       2 << 20,
	BaseLevelSize:       8 << 20,
	TableSizeMultiplier: 2,
	LevelSizeMultiplier: 10,
	MaxLevels:           7,
	NumGoroutines:       8,
	MetricsEnabled:      true,

	NumCompactors:           4, // Run at least 2 compactors. Zero-th compactor prioritizes L0.
	NumLevelZeroTables:      5,
	NumLevelZeroTablesStall: 15,
	NumMemtables:            5,
	BloomFalsePositive:      0.01,
	BlockSize:               4 * 1024,
	SyncWrites:              false,
	NumVersionsToKeep:       1,
	CompactL0OnClose:        false,
	VerifyValueChecksum:     false,
	Compression:             options.Snappy,
	BlockCacheSize:          25 << 20,
	IndexCacheSize:          0,

	ValueThreshold:   1 << 20, // 1MB
	ValueLogFileSize: 1 << 29, // 512MB; 1<<30-1(1GB)

	ValueLogMaxEntries: 1000000,

	VLogPercentile:                0.0,
	ZSTDCompressionLevel:          1,
	EncryptionKey:                 []byte{},
	EncryptionKeyRotationDuration: 10 * 24 * time.Hour, // Default 10 days.
	DetectConflicts:               true,
	NamespaceOffset:               -1,
}

func BenchmarkTxnSet(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkDir)
	// -count=2 -benchtime=3s -failfast -benchmem
	// -count=5 -benchtime=100000x  -benchmem  -failfast
	//db, err := badger.Open(benchMarkOpt)
	db, err := badger.Open(badger.DefaultOptions(benchMarkDir))
	defer db.Close()
	if err != nil {
		fmt.Printf("open badger failed, err:%v\n", err)
		return
	}
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		//valSize := 127 + 1
		//valSize := 10<<20 + 1
		valSize := 64<<20 + 1
		txnSet(b, db, BuildEntry(key, uint64(valSize)), 0)
	}

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		txnGet(b, db, key)
		key = []byte(randStr(12))
		txnGetNoKey(b, db, key)
	}
}

func txnGet(b *testing.B, db *badger.DB, key []byte) {
	transaction := db.NewTransaction(false)
	_, err := transaction.Get(key)
	require.NoError(b, err)
}

func txnGetNoKey(b *testing.B, db *badger.DB, key []byte) {
	transaction := db.NewTransaction(false)
	_, err := transaction.Get(key)
	require.Error(b, err)
}

func txnSet(b *testing.B, kv *badger.DB, badgerEntry *badger.Entry, meta byte) {
	// 1.获得 readTs(只读模式下有用); 并初始化数据容器;
	txn := kv.NewTransaction(true)
	// 2.添加数据到当前事务的数据容器中; 冲突容器, 同key不同version的数据;
	require.NoError(b, txn.SetEntry(badgerEntry))
	// 3.提交前进行检测冲突, 把数据发送到lsm,设置当前事务失效;
	require.NoError(b, txn.Commit())
}

func clearDir(dir string) {
	_, err := os.Stat(dir)
	if err == nil {
		if err = os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		_ = fmt.Sprintf("create dir %s failed", dir)
	}
}

func BuildEntry(key []byte, valSize uint64) *badger.Entry {
	value := make([]byte, valSize)
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &badger.Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func randStr(length int) string {
	// 包括特殊字符,进行测试
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}
