/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

type oracle struct {
	isManaged       bool // Does not change value, so no locking required.
	detectConflicts bool // Determines if the txns should be checked for conflicts.

	sync.Mutex // For nextTxnTs and commits.
	// writeChLock lock is for ensuring that transactions go to the write
	// channel in the same order as their commit timestamps.
	writeChLock sync.Mutex
	nextTxnTs   uint64

	// Used to block NewTransaction, so all previous commits are visible to a new read.
	txnMark *y.WaterMark

	// Either of these is used to determine which versions can be permanently
	// discarded during compaction.
	discardTs uint64       // Used by ManagedDB.
	readMark  *y.WaterMark // Used by DB.

	// committedTxns contains all committed writes (contains fingerprints
	// of keys written and their latest commit counter).
	committedTxns []committedTxn
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	closer *z.Closer
}

type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[uint64]struct{}
}

func newOracle(opt Options) *oracle {
	orc := &oracle{
		isManaged:       opt.managedTxns,
		detectConflicts: opt.DetectConflicts,
		// We're not initializing nextTxnTs and readOnlyTs. It would be done after replay in Open.
		// WaterMarks must be 64-bit aligned for atomic package, hence we must use pointers here.
		// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
		readMark: &y.WaterMark{Name: "badger.PendingReads"},
		txnMark:  &y.WaterMark{Name: "badger.TxnTimestamp"},
		closer:   z.NewCloser(2),
	}
	// read 读取水位线
	orc.readMark.Init(orc.closer)
	// 事务提交 水位线
	orc.txnMark.Init(orc.closer)
	return orc
}

func (o *oracle) Stop() {
	o.closer.SignalAndWait()
}

func (o *oracle) readTs() uint64 {
	if o.isManaged {
		panic("ReadTs should not be retrieved for managed DB")
	}

	var readTs uint64
	o.Lock()
	readTs = o.nextTxnTs - 1
	// 装填水印, 底层会一直等待此水印, 直到被关闭,然后才会执行下一个 readTs;
	o.readMark.Begin(readTs)
	o.Unlock()

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree process.
	// 等待所有txn, 包括:没有冲突的,已经分配了但是没有提交的,正在写日志的;
	// Not waiting here could mean that some txns which have been committed would not be read.
	// 不再这里等待, 意味着 某些事务已经提交;
	err := o.txnMark.WaitForMark(context.Background(), readTs)
	y.Check(err)
	return readTs
}

func (o *oracle) nextTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextTxnTs
}

func (o *oracle) incrementNextTs() {
	o.Lock()
	defer o.Unlock()
	o.nextTxnTs++
}

// Any deleted or invalid versions at or below ts would be discarded during
// compaction to reclaim disk space in LSM tree and thence value log.
func (o *oracle) setDiscardTs(ts uint64) {
	o.Lock()
	defer o.Unlock()
	o.discardTs = ts
	o.cleanupCommittedTransactions()
}

func (o *oracle) discardAtOrBelow() uint64 {
	if o.isManaged {
		o.Lock()
		defer o.Unlock()
		return o.discardTs
	}
	return o.readMark.DoneUntil()
}

// hasConflict must be called while having a lock.
func (o *oracle) hasConflict(txn *Txn) bool {
	// 当前事务 没有读取任何数据, 直接返回;
	if len(txn.reads) == 0 {
		return false
	}
	// 判断当前事务是否与已提交的事务存在冲突;
	for _, committedTxn := range o.committedTxns {
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it (@mrjn).
		// readTs = nextTs-1
		// commitTs = nextTs; nextTs++;
		// 当前事务ts 大于 之前提交过的事务ts,说明当前事务是在上一个事务提交后创建的,可以通行;
		if committedTxn.ts <= txn.readTs {
			continue
		}
		// committedTxn.ts > txn.readTs; 说明后来的事务已经提交, 前面的事务准备提交前的检测;
		// 当前事务是 之前创建并未提交的 旧事务; 需要判断是否 读过数据;
		// 1. 没有读过数据: 即便是提交相同的key, 也可以进行提交;
		// 2. 读过数据: 1) 没有发生冲突, 可继续提交;
		//             2) 发生了冲突, 则需要重新创建事务;
		for _, ro := range txn.reads {
			// 判断之前的提交记录, 是否和如今的读取记录有冲突;
			if _, has := committedTxn.conflictKeys[ro]; has {
				return true
			}
		}
	}
	return false
}

func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	o.Lock()
	defer o.Unlock()

	// 只要当前是否没有读过数据,直接可提交;
	if o.hasConflict(txn) {
		return 0, true
	}

	// 走到这里说明是没有发生冲突;
	var commitTs uint64
	// 默认是 系统来管理事务;
	if !o.isManaged {
		// 结束掉当前的 txn.readTs 标记;
		o.doneRead(txn)
		// 1.先清除掉 之前的事务旧提交记录;
		o.cleanupCommittedTransactions()

		// This is the general case, when user doesn't specify the read and commit ts.
		// 这是用户未指定读取和提交 ts 时的一般情况;
		commitTs = o.nextTxnTs
		o.nextTxnTs++
		// doneRead, 和txnMark.Begin(ts);
		o.txnMark.Begin(commitTs)
	} else { // 写操作
		// If commitTs is set, use it instead.
		commitTs = txn.commitTs
	}

	y.AssertTrue(commitTs >= o.lastCleanupTs)

	// 数据层面开启了,事务冲突检测, 就把当前事务的数据添加进去;
	// 冲突仅仅是在其他事务读取过数据时, 才开始检测的,否者不进行进一步的检测;
	if o.detectConflicts {
		// We should ensure that txns are not added to o.committedTxns slice when
		// conflict detection is disabled otherwise this slice would keep growing.
		// 2.将自己添加进去
		o.committedTxns = append(o.committedTxns, committedTxn{
			ts:           commitTs,
			conflictKeys: txn.conflictKeys,
		})
	}
	return commitTs, false
}

func (o *oracle) doneRead(txn *Txn) {
	if !txn.doneRead {
		txn.doneRead = true
		o.readMark.Done(txn.readTs)
	}
}

func (o *oracle) cleanupCommittedTransactions() { // Must be called under o.Lock
	if !o.detectConflicts {
		// When detectConflicts is set to false, we do not store any
		// committedTxns and so there's nothing to clean up.
		return
	}
	// Same logic as discardAtOrBelow but unlocked
	var maxReadTs uint64
	if o.isManaged {
		maxReadTs = o.discardTs
	} else {
		// 默认读取水位线, 默认是事务提交水位线;
		maxReadTs = o.readMark.DoneUntil()
	}

	y.AssertTrue(maxReadTs >= o.lastCleanupTs)

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	if maxReadTs == o.lastCleanupTs {
		return
	}
	o.lastCleanupTs = maxReadTs

	tmp := o.committedTxns[:0]
	for _, committedTxn := range o.committedTxns {
		if committedTxn.ts <= maxReadTs {
			continue
		}
		tmp = append(tmp, committedTxn)
	}
	o.committedTxns = tmp
}

func (o *oracle) doneCommit(cts uint64) {
	if o.isManaged {
		// No need to update anything.
		return
	}
	o.txnMark.Done(cts)
}

// Txn represents a Badger transaction.
type Txn struct {
	readTs   uint64
	commitTs uint64
	size     int64
	count    int64
	db       *DB

	reads []uint64 // contains fingerprints of keys read.
	// contains fingerprints of keys written. This is used for conflict detection.
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	pendingWrites   map[string]*Entry // cache stores any writes done by txn. 在当前Txn未提交前,保存所有kv;
	duplicateWrites []*Entry          // Used in managed mode to store duplicate entries.

	numIterators atomic.Int32
	discarded    bool
	doneRead     bool
	update       bool // update is used to conditionally keep track of reads.
}

type pendingWritesIterator struct {
	entries  []*Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}

func (pi *pendingWritesIterator) Seek(key []byte) {
	key = y.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (pi *pendingWritesIterator) Key() []byte {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.KeyWithTs(entry.Key, pi.readTs)
}

func (pi *pendingWritesIterator) Value() y.ValueStruct {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.ValueStruct{
		Value:     entry.Value,
		Meta:      entry.meta,
		UserMeta:  entry.UserMeta,
		ExpiresAt: entry.ExpiresAt,
		Version:   pi.readTs,
	}
}

func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (pi *pendingWritesIterator) Close() error {
	return nil
}

func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		entries = append(entries, e)
	}
	// Number of pending writes per transaction shouldn't be too big in general.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
	}
}

func (txn *Txn) checkSize(e *Entry) error {
	count := txn.count + 1
	// Extra bytes for the version in key.
	size := txn.size + e.estimateSizeAndSetThreshold(txn.db.valueThreshold()) + 10
	// 现在内存中的事务的kv对树 大于 count; 直接返回错误;
	if count >= txn.db.opt.maxBatchCount || size >= txn.db.opt.maxBatchSize {
		return ErrTxnTooBig
	}
	txn.count, txn.size = count, size
	return nil
}

func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

func (txn *Txn) modify(e *Entry) error {
	const maxKeySize = 65000

	switch {
	case !txn.update:
		return ErrReadOnlyTxn
	case txn.discarded:
		return ErrDiscardedTxn
	case len(e.Key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(e.Key, badgerPrefix):
		return ErrInvalidKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	case int64(len(e.Value)) > txn.db.opt.ValueLogFileSize: // value 大于一个vlogFileSize了都;
		return exceedsSize("Value", txn.db.opt.ValueLogFileSize, e.Value)
	case txn.db.opt.InMemory && int64(len(e.Value)) > txn.db.valueThreshold():
		return exceedsSize("Value", txn.db.valueThreshold(), e.Value)
	}

	if err := txn.db.isBanned(e.Key); err != nil {
		return err
	}

	// 检查当前 Tx 是否还有空间存放, 并且设置 e.valThreshold;
	if err := txn.checkSize(e); err != nil {
		return err
	}

	// The txn.conflictKeys is used for conflict detection.
	// If conflict detection is disabled, we don't need to store key hashes in this map.
	// 判断整个事务管理器是否开启事务冲突检测; 假如开启的话, 那么当前事务在提交时,就会判断其他事务是否含有当前事务数据;
	if txn.db.opt.DetectConflicts { // 当前事务开启冲突检测; 默认都开启;
		fp := z.MemHash(e.Key)            // Avoid dealing with byte arrays. 避免处理字节数组;
		txn.conflictKeys[fp] = struct{}{} // 目的: 提交时,将本次涉及到的key和其他提交记录进行对比;
	}

	// If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
	// Add the entry to duplicateWrites only if both the entries have different versions.
	// For same versions, we will overwrite the existing entry.
	oldEntry, ok := txn.pendingWrites[string(e.Key)]
	if ok && oldEntry.version != e.version { // 什么情景下, 会出现版本不一致呢? 在未提交前, 版本默认都是0啊;
		// 存在旧版本,并且版本号不一致, 将旧的移动到duplicateWrites中,等候发落;
		txn.duplicateWrites = append(txn.duplicateWrites, oldEntry)
	}

	// 未提交前, 先预存在内存中,只保留最新的e;
	txn.pendingWrites[string(e.Key)] = e
	return nil
}

// Set adds a key-value pair to the database.
// It will return ErrReadOnlyTxn if update flag was set to false when creating the transaction.
//
// The current transaction keeps a reference to the key and val byte slice
// arguments. Users must not modify key and val until the end of the transaction.
func (txn *Txn) Set(key, val []byte) error {
	return txn.SetEntry(NewEntry(key, val))
}

// SetEntry takes an Entry struct and adds the key-value pair in the struct,
// along with other metadata to the database.
//
// The current transaction keeps a reference to the entry passed in argument.
// Users must not modify the entry until the end of the transaction.
func (txn *Txn) SetEntry(e *Entry) error {
	return txn.modify(e)
}

// Delete deletes a key.
//
// This is done by adding a delete marker for the key at commit timestamp.  Any
// reads happening before this timestamp would be unaffected. Any reads after
// this commit would see the deletion.
//
// The current transaction keeps a reference to the key byte slice argument.
// Users must not modify the key until the end of the transaction.
func (txn *Txn) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	return txn.modify(e)
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded { // 当前事务无效;
		return nil, ErrDiscardedTxn
	}

	if err := txn.db.isBanned(key); err != nil {
		return nil, err
	}

	item = new(Item)
	// 事务在 写模式下,说明是写入数据的事务;
	// 自己写过的, 先读自己的;
	if txn.update {
		// 原生 key;
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) {
			if isDeletedOrExpired(e.meta, e.ExpiresAt) {
				return nil, ErrKeyNotFound
			}
			// Fulfill from cache.
			item.meta = e.meta
			item.val = e.Value
			item.userMeta = e.UserMeta
			item.key = key
			item.status = prefetched
			item.version = txn.readTs
			item.expiresAt = e.ExpiresAt
			// We probably don't need to set db on item here.
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it internally.
		// 说明内存中不存在key, 需要lsm中读取key;
		// 当前事务提交时, 会根据此来判断;
		txn.addReadKey(key)
	}

	// 装上 max - Ts;
	keyMaxReadTs := y.KeyWithTs(key, txn.readTs)
	vs, err := txn.db.get(keyMaxReadTs)
	if err != nil {
		return nil, y.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.vptr = y.SafeCopy(item.vptr, vs.Value)
	item.txn = txn
	item.expiresAt = vs.ExpiresAt
	return item, nil
}

func (txn *Txn) addReadKey(key []byte) {
	if txn.update {
		fp := z.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp)
		txn.readsLock.Unlock()
	}
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if txn.numIterators.Load() > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true
	if !txn.db.orc.isManaged {
		txn.db.orc.doneRead(txn)
	}
}

func (txn *Txn) commitAndSend() (func() error, error) {
	orc := txn.db.orc
	// Ensure that the order in which we get the commit timestamp is the same as
	// the order in which we push these updates to the write channel.
	// So, we acquire a writeChLock before getting a commit timestamp,
	// and only release it after pushing the entries to it.
	orc.writeChLock.Lock()
	defer orc.writeChLock.Unlock()
	// 检测是否存在key冲突;  o.doneRead(txn);
	commitTs, conflict := orc.newCommitTs(txn)

	if conflict {
		// 存在冲突直接返回当前事务;
		return nil, ErrConflict
	}

	keepTogether := true // 是否把数据统一在一个 事务中;
	setVersion := func(e *Entry) {
		if e.version == 0 {
			e.version = commitTs
		} else {
			// 存在额外的版本号, 那就分开标志位分开;
			keepTogether = false
		}
	}

	for _, e := range txn.pendingWrites {
		setVersion(e)
	}

	// The duplicateWrites slice will be non-empty only if there are duplicate
	// entries with different versions.
	// 只有当存在不同版本的重复条目时，duplicateWrites片才不为空。
	for _, e := range txn.duplicateWrites {
		// 重复的旧条目也设置成相同版本? 只是value不同?
		setVersion(e)
	}

	entries := make([]*Entry, 0, len(txn.pendingWrites)+len(txn.duplicateWrites)+1)

	processEntry := func(e *Entry) {
		// Suffix the keys with commit ts, so the key versions are sorted in descending order of commit timestamp.
		// commitTs 递增, 因此 key version(max - commitTs) 递减;
		e.Key = y.KeyWithTs(e.Key, e.version)
		// Add bitTxn only if these entries are part of a transaction. We
		// support SetEntryAt(..) in managed mode which means a single
		// transaction can have entries with different timestamps. If entries
		// in a single transaction have different timestamps, we don't add the
		// transaction markers.
		if keepTogether {
			e.meta |= bitTxn
		}
		entries = append(entries, e)
	}

	// The following debug information is what led to determining the cause of
	// bank txn violation bug, and it took a whole bunch of effort to narrow it
	// down to here. So, keep this around for at least a couple of months.
	// var b strings.Builder
	// fmt.Fprintf(&b, "Read: %d. Commit: %d. reads: %v. writes: %v. Keys: ",
	// 	txn.readTs, commitTs, txn.reads, txn.conflictKeys)
	for _, e := range txn.pendingWrites {
		processEntry(e)
	}
	for _, e := range txn.duplicateWrites {
		processEntry(e)
	}

	// 都在一个事务的话, 设置一个事务结束语;
	if keepTogether {
		// CommitTs should not be zero if we're inserting transaction markers.
		y.AssertTrue(commitTs != 0)
		e := &Entry{
			Key:   y.KeyWithTs(txnKey, commitTs),
			Value: []byte(strconv.FormatUint(commitTs, 10)),
			meta:  bitFinTxn,
		}
		entries = append(entries, e)
	}

	// 发送到db的专门写通道中, db.writeCh <- req
	req, err := txn.db.sendToWriteCh(entries)
	if err != nil {
		orc.doneCommit(commitTs)
		return nil, err
	}

	ret := func() error {
		// 阻塞等待,写入lsm结果, 然后才允许 结束当前水印;
		err := req.Wait()
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		orc.doneCommit(commitTs)
		return err
	}
	return ret, nil
}

func (txn *Txn) commitPrecheck() error {
	if txn.discarded {
		return errors.New("Trying to commit a discarded txn")
	}
	keepTogether := true
	for _, e := range txn.pendingWrites {
		// 什么情况下会发生 e.version 有值的?
		if e.version != 0 {
			keepTogether = false
		}
	}

	// If keepTogether is True, it implies transaction markers will be added.
	// In that case, commitTs should not be never be zero. This might happen if
	// someone uses txn.Commit instead of txn.CommitAt in managed mode.  This
	// should happen only in managed mode. In normal mode, keepTogether will
	// always be true.
	if keepTogether && txn.db.opt.managedTxns && txn.commitTs == 0 { // 如果在手动模式下, commitTS 不可为0;
		return errors.New("CommitTs cannot be zero. Please use commitAt instead")
	}
	return nil
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// 5. If callback is provided, Badger will return immediately after checking
// for conflicts. Writes to the database will happen in the background.  If
// there is a conflict, an error will be returned and the callback will not
// run. If there are no conflicts, the callback will be called in the
// background upon successful completion of writes or any error during write.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
func (txn *Txn) Commit() error {
	// txn.conflictKeys can be zero if conflict detection is turned off.
	// So we should check txn.pendingWrites.
	//if len(txn.pendingWrites) == 0 {
	//	// Discard the transaction so that the read is marked done.
	//	txn.Discard()
	//	return nil
	//}

	// Pre check before discarding txn. 简单检查;
	if err := txn.commitPrecheck(); err != nil {
		return err
	}
	// 设置 当前 readTs.done()
	defer txn.Discard()

	// 开始新建 commit和发送数据;
	txnCb, err := txn.commitAndSend()
	if err != nil {
		return err
	}
	// If batchSet failed, LSM would not have been updated. So, no need to rollback anything.

	// TODO: What if some of the txns successfully make it to value log, but others fail.
	// Nothing gets updated to LSM, until a restart happens.
	return txnCb()
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func runTxnCallback(cb *txnCb) {
	switch {
	case cb == nil:
		panic("txn callback is nil")
	case cb.user == nil:
		panic("Must have caught a nil callback for txn.CommitWith")
	case cb.err != nil:
		cb.user(cb.err)
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	default:
		cb.user(nil)
	}
}

// CommitWith acts like Commit, but takes a callback, which gets run via a
// goroutine to avoid blocking this function. The callback is guaranteed to run,
// so it is safe to increment sync.WaitGroup before calling CommitWith, and
// decrementing it in the callback; to block until all callbacks are run.
func (txn *Txn) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if len(txn.pendingWrites) == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		// Discard the transaction so that the read is marked done.
		txn.Discard()
		return
	}

	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil {
		cb(err)
		return
	}

	defer txn.Discard()

	commitCb, err := txn.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txn) ReadTs() uint64 {
	return txn.readTs
}

// NewTransaction creates a new transaction. Badger supports concurrent execution of transactions,
// providing serializable snapshot isolation, avoiding write skews. Badger achieves this by tracking
// the keys read and at Commit time, ensuring that these read keys weren't concurrently modified by
// another transaction.
//
// For read-only transactions, set update to false. In this mode, we don't track the rows read for
// any changes. Thus, any long running iterations done in this mode wouldn't pay this overhead.
//
// Running transactions concurrently is OK. However, a transaction itself isn't thread safe, and
// should only be run serially. It doesn't matter if a transaction is created by one goroutine and
// passed down to other, as long as the Txn APIs are called serially.
//
// When you create a new transaction, it is absolutely essential to call
// Discard(). This should be done irrespective of what the update param is set
// to. Commit API internally runs Discard, but running it twice wouldn't cause
// any issues.
//
//	txn := db.NewTransaction(false)
//	defer txn.Discard()
//	// Call various APIs.
func (db *DB) NewTransaction(update bool) *Txn {
	return db.newTransaction(update, false)
}

func (db *DB) newTransaction(update, isManaged bool) *Txn {
	if db.opt.ReadOnly && update {
		// DB is read-only, force read-only transaction.
		update = false
	}

	txn := &Txn{
		update: update,
		db:     db,
		count:  1,                       // One extra entry for BitFin.
		size:   int64(len(txnKey) + 10), // Some buffer for the extra entry.
	}
	// 当前事务涉及到写;
	if update {
		if db.opt.DetectConflicts { // 开启冲突检查;
			txn.conflictKeys = make(map[uint64]struct{})
		}
		txn.pendingWrites = make(map[string]*Entry)
	}
	// 是否 手动管理. 默认是自动管理;
	if !isManaged {
		// readTs = nextTs - 1;
		txn.readTs = db.orc.readTs()
	}
	return txn
}

// View executes a function creating and managing a read-only transaction for the user.
// Error returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	var txn *Txn
	if db.opt.managedTxns {
		// 最大的 版本;
		txn = db.NewTransactionAt(math.MaxUint64, false)
	} else {
		// 紧接着的版本;
		txn = db.NewTransaction(false)
	}
	defer txn.Discard()
	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
// Update cannot be used with managed transactions.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	if db.opt.managedTxns {
		panic("Update can only be used with managedDB=false.")
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
