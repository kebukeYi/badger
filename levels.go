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
	stderrors "errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	otrace "go.opencensus.io/trace"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

type levelsController struct {
	nextFileID atomic.Uint64
	l0stallsMs atomic.Int64

	// The following are initialized once and const.
	levels []*levelHandler
	kv     *DB

	cstatus compactStatus
}

// revertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest. idMap is a set of table file id's that were read from the directory
// listing.
func revertToManifest(kv *DB, mf *Manifest, idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.Tables[id]; !ok {
			kv.opt.Debugf("Table file %d not referenced in MANIFEST\n", id)
			filename := table.NewFilename(id, kv.opt.Dir)
			if err := os.Remove(filename); err != nil {
				return y.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

func newLevelsController(db *DB, mf *Manifest) (*levelsController, error) {
	y.AssertTrue(db.opt.NumLevelZeroTablesStall > db.opt.NumLevelZeroTables)
	s := &levelsController{
		kv:     db,
		levels: make([]*levelHandler, db.opt.MaxLevels),
	}
	s.cstatus.tables = make(map[uint64]struct{})
	s.cstatus.levels = make([]*levelCompactStatus, db.opt.MaxLevels)

	for i := 0; i < db.opt.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(db, i)
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	if db.opt.InMemory {
		return s, nil
	}
	// Compare manifest against directory, check for existent/non-existent files, and remove.
	if err := revertToManifest(db, mf, getIDMap(db.opt.Dir)); err != nil {
		return nil, err
	}

	var mu sync.Mutex
	tables := make([][]*table.Table, db.opt.MaxLevels)
	var maxFileID uint64

	// We found that using 3 goroutines allows disk throughput to be utilized to its max.
	// Disk utilization is the main thing we should focus on, while trying to read the data. That's
	// the one factor that remains constant between HDD and SSD.
	throttle := y.NewThrottle(3)

	start := time.Now()
	var numOpened atomic.Int32
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	for fileID, tf := range mf.Tables {
		fname := table.NewFilename(fileID, db.opt.Dir)
		select {
		case <-tick.C:
			db.opt.Infof("%d tables out of %d opened in %s\n", numOpened.Load(),
				len(mf.Tables), time.Since(start).Round(time.Millisecond))
		default:
		}
		if err := throttle.Do(); err != nil {
			closeAllTables(tables)
			return nil, err
		}
		if fileID > maxFileID {
			maxFileID = fileID
		}
		go func(fname string, tf TableManifest) {
			var rerr error
			defer func() {
				throttle.Done(rerr)
				numOpened.Add(1)
			}()
			dk, err := db.registry.DataKey(tf.KeyID)
			if err != nil {
				rerr = y.Wrapf(err, "Error while reading datakey")
				return
			}
			topt := buildTableOptions(db)
			// Explicitly set Compression and DataKey based on how the table was generated.
			topt.Compression = tf.Compression
			topt.DataKey = dk

			mf, err := z.OpenMmapFile(fname, db.opt.getFileFlags(), 0)
			if err != nil {
				rerr = y.Wrapf(err, "Opening file: %q", fname)
				return
			}
			t, err := table.OpenTable(mf, topt)
			if err != nil {
				if strings.HasPrefix(err.Error(), "CHECKSUM_MISMATCH:") {
					db.opt.Errorf(err.Error())
					db.opt.Errorf("Ignoring table %s", mf.Fd.Name())
					// Do not set rerr. We will continue without this table.
				} else {
					rerr = y.Wrapf(err, "Opening table: %q", fname)
				}
				return
			}
			mu.Lock()
			tables[tf.Level] = append(tables[tf.Level], t)
			mu.Unlock()
		}(fname, tf)
	}
	//
	if err := throttle.Finish(); err != nil {
		closeAllTables(tables)
		return nil, err
	}
	db.opt.Infof("All %d tables opened in %s\n", numOpened.Load(), time.Since(start).Round(time.Millisecond))
	s.nextFileID.Store(maxFileID + 1)
	for i, tbls := range tables {
		s.levels[i].initTables(tbls)
	}

	// Make sure key ranges do not overlap etc.
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, y.Wrap(err, "Level validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	if err := syncDir(db.opt.Dir); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil
}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef()
// because that would delete the underlying files.)  We ignore errors, which is OK because tables
// are read-only.
func closeAllTables(tables [][]*table.Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close(-1)
		}
	}
}

func (s *levelsController) cleanupLevels() error {
	var firstErr error
	for _, l := range s.levels {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dropTree picks all tables from all levels, creates a manifest changeset,
// applies it, and then decrements the refs of these tables, which would result
// in their deletion.
func (s *levelsController) dropTree() (int, error) {
	// First pick all tables, so we can create a manifest changelog.
	var all []*table.Table
	for _, l := range s.levels {
		l.RLock()
		all = append(all, l.tables...)
		l.RUnlock()
	}
	if len(all) == 0 {
		return 0, nil
	}

	// Generate the manifest changes.
	changes := []*pb.ManifestChange{}
	for _, table := range all {
		// Add a delete change only if the table is not in memory.
		if !table.IsInmemory {
			changes = append(changes, newDeleteChange(table.ID()))
		}
	}
	changeSet := pb.ManifestChangeSet{Changes: changes}
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return 0, err
	}

	// Now that manifest has been successfully written, we can delete the tables.
	for _, l := range s.levels {
		l.Lock()
		l.totalSize = 0
		l.tables = l.tables[:0]
		l.Unlock()
	}
	for _, table := range all {
		if err := table.DecrRef(); err != nil {
			return 0, err
		}
	}
	return len(all), nil
}

// dropPrefix runs a L0->L1 compaction, and then runs same level compaction on the rest of the
// levels. For L0->L1 compaction, it runs compactions normally, but skips over
// all the keys with the provided prefix.
// For Li->Li compactions, it picks up the tables which would have the prefix. The
// tables who only have keys with this prefix are quickly dropped. The ones which have other keys
// are run through MergeIterator and compacted to create new tables. All the mechanisms of
// compactions apply, i.e. level sizes and MANIFEST are updated as in the normal flow.
func (s *levelsController) dropPrefixes(prefixes [][]byte) error {
	opt := s.kv.opt
	// Iterate levels in the reverse order because if we were to iterate from
	// lower level (say level 0) to a higher level (say level 3) we could have
	// a state in which level 0 is compacted and an older version of a key exists in lower level.
	// At this point, if someone creates an iterator, they would see an old
	// value for a key from lower levels. Iterating in reverse order ensures we
	// drop the oldest data first so that lookups never return stale data.
	for i := len(s.levels) - 1; i >= 0; i-- {
		l := s.levels[i]

		l.RLock()
		if l.level == 0 {
			size := len(l.tables)
			l.RUnlock()

			if size > 0 {
				cp := compactionPriority{
					level: 0,
					score: 1.74,
					// A unique number greater than 1.0 does two things. Helps identify this
					// function in logs, and forces a compaction.
					dropPrefixes: prefixes,
				}
				if err := s.doCompact(174, cp); err != nil {
					opt.Warningf("While compacting level 0: %v", err)
					return nil
				}
			}
			continue
		}

		// Build a list of compaction tableGroups affecting all the prefixes we
		// need to drop. We need to build tableGroups that satisfy the invariant that
		// bottom tables are consecutive.
		// tableGroup contains groups of consecutive tables.
		var tableGroups [][]*table.Table
		var tableGroup []*table.Table

		finishGroup := func() {
			if len(tableGroup) > 0 {
				tableGroups = append(tableGroups, tableGroup)
				tableGroup = nil
			}
		}

		for _, table := range l.tables {
			if containsAnyPrefixes(table, prefixes) {
				tableGroup = append(tableGroup, table)
			} else {
				finishGroup()
			}
		}
		finishGroup()

		l.RUnlock()

		if len(tableGroups) == 0 {
			continue
		}
		_, span := otrace.StartSpan(context.Background(), "Badger.Compaction")
		span.Annotatef(nil, "Compaction level: %v", l.level)
		span.Annotatef(nil, "Drop Prefixes: %v", prefixes)
		defer span.End()
		opt.Infof("Dropping prefix at level %d (%d tableGroups)", l.level, len(tableGroups))
		for _, operation := range tableGroups {
			cd := compactDef{
				span:         span,
				thisLevel:    l,
				nextLevel:    l,
				top:          nil,
				bot:          operation,
				dropPrefixes: prefixes,
				t:            s.levelTargets(),
			}
			cd.t.baseLevel = l.level
			if err := s.runCompactDef(-1, l.level, cd); err != nil {
				opt.Warningf("While running compact def: %+v. Error: %v", cd, err)
				return err
			}
		}
	}
	return nil
}

func (s *levelsController) startCompact(lc *z.Closer) {
	n := s.kv.opt.NumCompactors
	lc.AddRunning(n - 1)
	for coroutineID := 0; coroutineID < n; coroutineID++ {
		// coroutineID
		go s.runCompactor(coroutineID, lc)
	}
}

type targets struct {
	baseLevel int
	targetSz  []int64
	fileSz    []int64
}

// levelTargets calculates the targets for levels in the LSM tree. The idea comes from Dynamic Level
// Sizes ( https://rocksdb.org/blog/2015/07/23/dynamic-level.html ) in RocksDB. The sizes of levels
// are calculated based on the size of the lowest level, typically L6. So, if L6 size is 1GB, then
// L5 target size is 100MB, L4 target size is 10MB and so on.
//
// L0 files don't automatically go to L1. Instead, they get compacted to Lbase, where Lbase is
// chosen based on the first level which is non-empty from top (check L1 through L6). For an empty
// DB, that would be L6.  So, L0 compactions go to L6, then L5, L4 and so on.
//
// Lbase is advanced to the upper levels when its target size exceeds BaseLevelSize. For
// example, when L6 reaches 1.1GB, then L4 target sizes becomes 11MB, thus exceeding the
// BaseLevelSize of 10MB. L3 would then become the new Lbase, with a target size of 1MB <
// BaseLevelSize.
func (s *levelsController) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < s.kv.opt.BaseLevelSize {
			return s.kv.opt.BaseLevelSize
		}
		return sz
	}

	t := targets{
		targetSz: make([]int64, len(s.levels)),
		fileSz:   make([]int64, len(s.levels)),
	}
	// DB size is the size of the last level.
	dbSize := s.lastLevel().getTotalSize()
	for i := len(s.levels) - 1; i > 0; i-- {
		ltarget := adjust(dbSize)
		t.targetSz[i] = ltarget
		if t.baseLevel == 0 && ltarget <= s.kv.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(s.kv.opt.LevelSizeMultiplier)
	}

	tsz := s.kv.opt.BaseTableSize
	for i := 0; i < len(s.levels); i++ {
		if i == 0 {
			// Use MemTableSize for Level 0. Because at Level 0, we stop compactions based on the
			// number of tables, not the size of the level. So, having a 1:1 size ratio between
			// memtable size and the size of L0 files is better than churning out 32 files per
			// memtable (assuming 64MB MemTableSize and 2MB BaseTableSize).
			t.fileSz[i] = s.kv.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(s.kv.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// Bring the base level down to the last empty level.
	for i := t.baseLevel + 1; i < len(s.levels)-1; i++ {
		if s.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// If the base level is empty and the next level size is less than the target size,
	// pick the next level as the base level.
	b := t.baseLevel
	lvl := s.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

func (s *levelsController) runCompactor(coroutineID int, lc *z.Closer) {
	defer lc.Done()
	// 上来来个随机启动;
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-lc.HasBeenClosed():
		randomDelay.Stop()
		return
	}

	moveL0toFront := func(prios []compactionPriority) []compactionPriority {
		idx := -1
		for i, p := range prios {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		if idx > 0 {
			out := append([]compactionPriority{}, prios[idx])
			out = append(out, prios[:idx]...)
			out = append(out, prios[idx+1:]...)
			return out
		}
		return prios
	}
	run := func(p compactionPriority) bool {
		err := s.doCompact(coroutineID, p)
		switch err {
		case nil:
			return true
		case errFillTables:
			// pass
		default:
			s.kv.opt.Warningf("While running doCompact: %v\n", err)
		}
		return false
	}
	var priosBuffer []compactionPriority // 采用复制
	runOnce := func() bool {
		// 返回各个层的合并分数;
		prios := s.pickCompactLevels(priosBuffer)
		defer func() {
			priosBuffer = prios
		}()
		if coroutineID == 0 {
			// Worker ID zero prefers to compact L0 always.
			prios = moveL0toFront(prios)
		}
		for _, p := range prios {
			if coroutineID == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
			} else if p.adjusted < 1.0 {
				break
			}
			// Try to run the compaction.
			if run(p) {
				return true
			}
		}
		return false
	}
	tryLmaxToLmaxCompaction := func() {
		p := compactionPriority{
			level: s.lastLevel().level,
			t:     s.levelTargets(), // 重新获得每层的情况;
		}
		run(p)
	}
	count := 0
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	// 周期 选择 LMax -> LMax; 启动;
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			count++
			// Each ticker is 50ms so 50 * 200 = 10 seconds.
			// 每 10 秒 有概率执行 LMax -> LMax;
			if s.kv.opt.LmaxCompaction && coroutineID == 2 && count >= 200 {
				tryLmaxToLmaxCompaction()
				count = 0
			} else {
				runOnce()
			}
		case <-lc.HasBeenClosed():
			return
		}
	}
}

type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

func (s *levelsController) lastLevel() *levelHandler {
	return s.levels[len(s.levels)-1]
}

// pickCompactLevel determines which level to compact.
// Based on: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
// It tries to reuse priosBuffer to reduce memory allocation,
// passing nil is acceptable, then new memory will be allocated.
func (s *levelsController) pickCompactLevels(priosBuffer []compactionPriority) (prios []compactionPriority) {
	// 获得每层的实际情况和计算出期望大小;
	t := s.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// Grow buffer to fit all levels.
	if cap(priosBuffer) < len(s.levels) {
		priosBuffer = make([]compactionPriority, 0, len(s.levels))
	}
	prios = priosBuffer[:0]

	// Add L0 priority based on the number of tables.
	addPriority(0, float64(s.levels[0].numTables())/float64(s.kv.opt.NumLevelZeroTables))

	// All other levels use size to calculate priority.
	for i := 1; i < len(s.levels); i++ {
		// Don't consider those tables that are already being compacted right now.
		delSize := s.cstatus.delSize(i)

		l := s.levels[i]
		sz := l.getTotalSize() - delSize
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	y.AssertTrue(len(prios) == len(s.levels))

	// The following code is borrowed from PebbleDB and results in healthier LSM tree structure.
	// If Li-1 has score > 1.0, then we'll divide Li-1 score by Li. If Li score is >= 1.0, then Li-1
	// score is reduced, which means we'll prioritize the compaction of lower levels (L5, L4 and so
	// on) over the higher levels (L0, L1 and so on). On the other hand, if Li score is < 1.0, then
	// we'll increase the priority of Li-1.
	// Overall what this means is, if the bottom level is already overflowing, then de-prioritize
	// compaction of the above level. If the bottom level is not full, then increase the priority of
	// above level.
	// 如果 Li-1 的score > 1.0 ; 那么 Li-1 = Li-1/Li; 否则下探;
	// 假如 Li.score >= 1.0, Li-1.score就会变小, 那么我们就倾向于 Li层的压缩;
	// 假如 Li.score < 1.0,  Li-1.score就会变大, 那么我们就倾向于 Li-1层的压缩;
	var prevLevel int
	for level := t.baseLevel; level < len(s.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// Avoid absurdly large scores by placing a floor on the score that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// Pick all the levels whose original score is >= 1.0, irrespective of their adjusted score.
	// We'll still sort them by their adjusted score below. Having both these scores allows us to
	// make better decisions about compacting L0. If we see a score >= 1.0, we can do L0->L0
	// compactions. If the adjusted score >= 1.0, then we can do L0->Lbase compactions.
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// Sort by the adjusted score.
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// checkOverlap checks if the given tables overlap with any level from the given "lev" onwards.
func (s *levelsController) checkOverlap(tables []*table.Table, lev int) bool {
	kr := getKeyRange(tables...) // 给定的 table 区间
	for i, lh := range s.levels {
		if i < lev { // Skip upper levels.  跳过 低于本层的;
			continue
		}
		lh.RLock()
		// 判断当前 level 是否存在区间;
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// subcompact runs a single sub-compaction, iterating over the specified key-range only.
//
// We use splits to do a single compaction concurrently. If we have >= 3 tables
// involved in the bottom level during compaction, we choose key ranges to
// split the main compaction up into sub-compactions. Each sub-compaction runs
// concurrently, only iterating over the provided key range, generating tables.
// This speeds up the compaction significantly.
func (s *levelsController) subcompact(it y.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *y.Throttle, res chan<- *table.Table) {

	// Check overlap of the top level with the levels which are not being
	// compacted in this compaction.
	// 从 cd.nextLevel.level+1层的表 开始判断本次合并涉及到的表, 是否和其存在重叠区间;
	// 存在的话, 那就不能轻易删除数据,比如 delete 类型数据;
	hasOverlap := s.checkOverlap(cd.allTables(), cd.nextLevel.level+1)

	// Pick a discard ts, so we can discard versions below this ts. We should
	// never discard any versions starting from above this timestamp, because
	// that would affect the snapshot view guarantee provided by transactions.
	// 寻找可丢弃数据的时间戳;
	discardTs := s.kv.orc.discardAtOrBelow()

	// Try to collect stats so that we can inform value log about GC.
	// That would help us find which value log file should be GCed.
	discardStats := make(map[uint32]int64)
	updateStats := func(vs y.ValueStruct) {
		// We don't need to store/update discard stats when badger is running in Disk-less mode.
		if s.kv.opt.InMemory {
			return
		}
		if vs.Meta&bitValuePointer > 0 {
			var vp valuePointer
			vp.Decode(vs.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	// exceedsAllowedOverlap returns true if the given key range would overlap with more than 10
	// tables from level below nextLevel (nextLevel+1).
	// This helps avoid generating tables at Li with huge overlaps with Li+1.
	// 这有助于避免在Li上生成与Li+1有大量重叠的表;
	exceedsAllowedOverlap := func(kr keyRange) bool {
		n2n := cd.nextLevel.level + 1
		if n2n <= 1 || n2n >= len(s.levels) {
			return false
		}
		n2nl := s.levels[n2n]
		n2nl.RLock()
		defer n2nl.RUnlock()
		// 和将要参与合并的下一层 table 数量重叠次数过多;
		l, r := n2nl.overlappingTables(levelHandlerRLocked{}, kr)
		return r-l >= 10
	}

	var (
		lastKey, skipKey       []byte
		numBuilds, numVersions int
		// Denotes if the first key is a series of duplicate keys had "DiscardEarlierVersions" set
		firstKeyHasDiscardSet bool
	)

	addKeys := func(builder *table.Builder) {
		timeStart := time.Now()
		var numKeys, numSkips uint64
		var rangeCheck int
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			// See if we need to skip the prefix.
			if len(cd.dropPrefixes) > 0 && hasAnyPrefixes(it.Key(), cd.dropPrefixes) {
				numSkips++
				// 遇见定义要跳过的 key, 告诉vlog;
				updateStats(it.Value()) // 更新 .vlogFile 统计;
				continue
			}

			// See if we need to skip this key.
			// 1. 上一个是删除类型消息, 那么之后的key就可以跳过了;
			// 2. 上一个键已经达到默认key版本保留数量, 剩余的key也可以跳过了;
			if len(skipKey) > 0 {
				// 不记版本, 直接跳过 原生key相同的;
				if y.SameKey(it.Key(), skipKey) {
					numSkips++
					updateStats(it.Value())
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			// 前后两个 原生 key 不相同: 还可以来设置 是否可删除 后续遍历到的 早期版本数据;
			if !y.SameKey(it.Key(), lastKey) {
				firstKeyHasDiscardSet = false
				// 判断是否越界;
				if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// Only break if we are on a different key, and have reached capacity.
					// We want to ensure that all versions of the key are stored in the same sstable,
					// and not divided across multiple tables at the same level.
					// 我们只想让相同key的不同版本数据全都存储在同一个. sst 中;
					break
				}

				lastKey = y.SafeCopy(lastKey, it.Key())
				numVersions = 0
				// 判断是否 可以丢弃早期版本键;
				firstKeyHasDiscardSet = it.Value().Meta&bitDiscardEarlierVersions > 0

				// 没有越界 && 不是可丢弃类型的消息 && 不同的key;
				if len(tableKr.left) == 0 {
					// 第一个出现不相同的 key, 保存起来;
					tableKr.left = y.SafeCopy(tableKr.left, it.Key())
				}
				// 最新的不相同的key, 保存起来;
				tableKr.right = lastKey

				rangeCheck++
				// todo 合并优化点: 两者不相同的key, 达到5000个;
				if rangeCheck%5000 == 0 {
					// This table's range exceeds the allowed range overlap with the level after next.
					// 该表的范围超过了允许的与next之后的级别重叠的范围;
					// So, we stop writing to this table. If we don't do this, then we end up
					// doing very expensive compactions involving too many tables.
					// 否则的话, 我们将会做非常昂贵的合并操作, 包含过多的表;
					// To amortize the cost of this check, we do it only every N keys.
					if exceedsAllowedOverlap(tableKr) {
						// s.kv.opt.Debugf("L%d -> L%d Breaking due to exceedsAllowedOverlap with
						// kr: %s\n", cd.thisLevel.level, cd.nextLevel.level, tableKr)
						// 直接跳出循环, 去新建 .sst 文件;
						break
					}
				}
			}

			// aaa:1  bbb:2  ccc:4  ddd:3
			// 前后两个 原生 key 不相同
			// aaa:10  aaa:9  aaa:6:delete  aaa:3  aaa:1
			// 前后两个 原生 key 相同
			vs := it.Value()
			version := y.ParseTs(it.Key())
			// 判断是否是 删除墓碑类型的消息 或者 到期的数据;
			isExpired := isDeletedOrExpired(vs.Meta, vs.ExpiresAt)

			// Do not discard entries inserted by merge operator.
			// These entries will be discarded once they're merged.
			// 不要丢弃由合并操作符插入的项. 这些条目会在未来某一个时刻合并时被丢弃.
			// 当前entry.version <= 可丢弃version; 说明可丢弃;
			// 不是可合并的; 并且又不是 合并类型的消息;
			if version <= discardTs && vs.Meta&bitMergeEntry == 0 {
				// Keep track of the number of versions encountered for this key.
				// Only consider the versions which are below the minReadTs,
				// otherwise, we might end up discarding the only valid version for a running transaction.
				// 跟踪此键遇到的低版本数量,只考虑 minReadTs 以下的版本;
				// 否则, 我们可能最终会丢弃正在运行的事务的唯一有效版本;
				numVersions++

				// 保留当前版本，并丢弃所有下一个版本:
				// Keep the current version and discard all the next versions if
				// - The `discardEarlierVersions` bit is set OR  允许丢弃早期版本数据
				// - We've already processed `NumVersionsToKeep` number of versions
				// (including the current item being processed)包括现在正在遍历的;
				lastValidVersion := vs.Meta&bitDiscardEarlierVersions > 0 ||
					numVersions == s.kv.opt.NumVersionsToKeep // 默认是1;

				// 过期,删除类型 || 最后一个有效版本的key了, 后续遍历到的可丢弃掉了; 并且会通知 vlog;
				if isExpired || lastValidVersion {
					// If this version of the key is deleted or expired,
					// skip all the rest of the versions.
					// 如果当前遍历到 删除类型的消息, 那么就把后续遍历到的低版本数据可删除掉了;
					// Ensure that we're only removing versions below readTs.
					// 确保我们只删除 readTs 以下的版本;
					skipKey = y.SafeCopy(skipKey, it.Key())

					switch {
					// Add the key to the table only if it has not expired.
					// We don't want to add the deleted/expired keys.
					case !isExpired && lastValidVersion:
						// 不是删除版本 && 但是key版本刚刚够了,当前key不用删除, 把下一个的key清除即可;
						// Add this key. We have set skipKey, so the following key versions
						// would be skipped.
					case hasOverlap:
						// 是删除版本 || (但是key版本保留可够也可不够 && 重叠区间太多);
						// 但是存在重叠区间, 因此不能轻易把 删除标记给清理掉,但是后续遍历到的低版本可skip;
						// If this key range has overlap with lower levels, then keep the deletion
						// marker with the latest version, discarding the rest.
						// 如果此键范围与较低级别重叠, 则保留最新版本的删除标记, 丢弃其余部分;
						// We have set skipKey, so the following key versions would be skipped.
					default:
						// 没有出现重复 || 当前key是删除类型 || (但是key版本保留可够也可不够 && 重叠区间不多);
						// If no overlap, we can skip all the versions, by continuing here.
						numSkips++
						updateStats(vs) // todo 更新 vlogFile 统计信息;
						continue        // Skip adding this key.
					}
				}
			}

			// version > discardTs; 不跳过有效key;
			numKeys++
			var vp valuePointer
			if vs.Meta&bitValuePointer > 0 {
				vp.Decode(vs.Value)
			}
			switch {
			case firstKeyHasDiscardSet:
				// This key is same as the last key which had "DiscardEarlierVersions" set.
				// 这个键与最后一个设置了“DiscardEarlierVersions”的键相同;
				// The next compactions will drop this key if its ts > discardTs (of the next compaction).
				builder.AddStaleKey(it.Key(), vs, vp.Len) // 本次保留, 下一次就会被删除掉;
				fmt.Printf("compact:firstKeyHasDiscardSet:key: %s, skipKey:%s \n", it.Key(), skipKey)
			case isExpired:
				// If the key is expired, the next compaction will drop it if its ts > discardTs (of the next compaction).
				builder.AddStaleKey(it.Key(), vs, vp.Len) // 本次保留, 下一次就会被删除掉;
				fmt.Printf("compact:isExpired:key: %s, skipKey:%s \n", it.Key(), skipKey)
			default:
				// 正常数据;
				builder.Add(it.Key(), vs, vp.Len)
				fmt.Printf("compact:normal:key: %s, skipKey:%s \n", it.Key(), skipKey)
			}
		} // for over...

		s.kv.opt.Debugf("[%d] LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v",
			cd.compactorId, numKeys, numSkips, time.Since(timeStart).Round(time.Millisecond))
	} // End of function: addKeys()

	if len(kr.left) > 0 {
		// 归并迭代器 定位到区间的开始;
		it.Seek(kr.left)
	} else {
		it.Rewind()
	}
	for it.Valid() {
		// 如果存在 右区间 && 当前key > 右区间, 说明遍历到头, 跳出循环;
		if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
			break
		}

		bopts := buildTableOptions(s.kv)
		// Set TableSize to the target file size for that level.
		bopts.TableSize = uint64(cd.t.fileSz[cd.nextLevel.level])
		builder := table.NewTableBuilder(bopts)

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.Empty() {
			// Cleanup builder resources:
			builder.Finish()
			builder.Close()
			continue
		}

		numBuilds++
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}

		go func(builder *table.Builder, fileID uint64) {
			var err error
			defer inflightBuilders.Done(err)
			defer builder.Close()

			var tbl *table.Table
			if s.kv.opt.InMemory {
				tbl, err = table.OpenInMemoryTable(builder.Finish(), fileID, &bopts)
			} else {
				fname := table.NewFilename(fileID, s.kv.opt.Dir)
				// 创建表;
				tbl, err = table.CreateTable(fname, builder)
			}

			// If we couldn't build the table, return fast.
			if err != nil {
				return
			}
			res <- tbl
		}(builder, s.reserveFileID())
	}
	// 向vlog发送统计的失效数据;
	s.kv.vlog.updateDiscardStats(discardStats)
	s.kv.opt.Debugf("Discard stats: %v", discardStats)
}

// compactBuildTables merges topTables and botTables to form a list of new tables.
func (s *levelsController) compactBuildTables(
	lev int, cd compactDef) ([]*table.Table, func() error, error) {

	topTables := cd.top
	botTables := cd.bot

	numTables := int64(len(topTables) + len(botTables))
	y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, numTables)
	defer y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, -numTables)

	cd.span.Annotatef(nil, "Top tables count: %v Bottom tables count: %v", len(topTables), len(botTables))

	// 是否需要立即删除当前 table;
	keepTable := func(t *table.Table) bool {
		for _, prefix := range cd.dropPrefixes {
			if bytes.HasPrefix(t.Smallest(), prefix) && bytes.HasPrefix(t.Biggest(), prefix) {
				// All the keys in this table have the dropPrefix. So, this
				// table does not need to be in the iterator and can be
				// dropped immediately.
				return false
			}
		}
		return true
	}

	var validBotTables []*table.Table
	for _, table := range botTables {
		if keepTable(table) {
			validBotTables = append(validBotTables, table)
		}
	}

	newIterator := func() []y.Iterator {
		// Create iterators across all the tables involved first.
		var iters []y.Iterator
		switch {
		case lev == 0:
			iters = appendIteratorsReversed(iters, topTables, table.NOCACHE)
		case len(topTables) > 0:
			y.AssertTrue(len(topTables) == 1)
			iters = []y.Iterator{topTables[0].NewIterator(table.NOCACHE)}
		}
		// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
		return append(iters, table.NewConcatIterator(validBotTables, table.NOCACHE))
	}

	res := make(chan *table.Table, 3)
	inflightBuilders := y.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil {
			s.kv.opt.Errorf("cannot start subcompaction: %+v", err)
			return nil, nil, err
		}
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := table.NewMergeIterator(newIterator(), false)
			defer it.Close()
			// for 循环执行子机划
			s.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}

	var newTables []*table.Table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// Wait for all table builders to finish and also for newTables accumulator to finish.
	err := inflightBuilders.Finish()
	close(res)
	wg.Wait() // Wait for all tables to be picked up.

	if err == nil {
		// Ensure created files' directory entries are visible.  We don't mind the extra latency
		// from not doing this ASAP after all file creation has finished because this is a
		// background operation.
		err = s.kv.syncDir(s.kv.opt.Dir)
	}

	if err != nil {
		// An error happened.  Delete all the newly created table files (by calling DecrRef
		// -- we're the only holders of a ref).
		_ = decrRefs(newTables)
		return nil, nil, y.Wrapf(err, "while running compactions for: %+v", cd)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return y.CompareKeys(newTables[i].Biggest(), newTables[j].Biggest()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func buildChangeSet(cd *compactDef, newTables []*table.Table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes,
			newCreateChange(table.ID(), cd.nextLevel.level, table.KeyID(), table.CompressionType()))
	}
	for _, table := range cd.top {
		// Add a delete change only if the table is not in memory.
		if !table.IsInmemory {
			changes = append(changes, newDeleteChange(table.ID()))
		}
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func hasAnyPrefixes(s []byte, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if bytes.HasPrefix(s, prefix) {
			return true
		}
	}

	return false
}

func containsPrefix(table *table.Table, prefix []byte) bool {
	smallValue := table.Smallest()
	largeValue := table.Biggest()
	if bytes.HasPrefix(smallValue, prefix) {
		return true
	}
	if bytes.HasPrefix(largeValue, prefix) {
		return true
	}
	isPresent := func() bool {
		ti := table.NewIterator(0)
		defer ti.Close()
		// In table iterator's Seek, we assume that key has version in last 8 bytes. We set
		// version=0 (ts=math.MaxUint64), so that we don't skip the key prefixed with prefix.
		ti.Seek(y.KeyWithTs(prefix, math.MaxUint64))
		return bytes.HasPrefix(ti.Key(), prefix)
	}

	if bytes.Compare(prefix, smallValue) > 0 &&
		bytes.Compare(prefix, largeValue) < 0 {
		// There may be a case when table contains [0x0000,...., 0xffff]. If we are searching for
		// k=0x0011, we should not directly infer that k is present. It may not be present.
		return isPresent()
	}

	return false
}

func containsAnyPrefixes(table *table.Table, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if containsPrefix(table, prefix) {
			return true
		}
	}

	return false
}

type compactDef struct {
	span *otrace.Span

	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table.Table
	bot []*table.Table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

// addSplits can allow us to run multiple sub-compactions in parallel across the split key ranges.
func (s *levelsController) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = y.Copy(right)
		cd.splits = append(cd.splits, skr)

		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// Right is assigned ts=0. The encoding ts bytes take MaxUint64-ts,
			// so, those with smaller TS will be considered larger for the same key.
			// Consider the following.
			// Top table is [A1...C3(deleted)]
			// bot table is [B1....C2]
			// It will generate a split [A1 ... C0], including any records of Key C.
			right := y.KeyWithTs(y.ParseKey(t.Biggest()), 0)
			addRange(right)
		}
	}
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

func (cd *compactDef) allTables() []*table.Table {
	ret := make([]*table.Table, 0, len(cd.top)+len(cd.bot))
	ret = append(ret, cd.top...)
	ret = append(ret, cd.bot...)
	return ret
}

func (s *levelsController) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// Only compactor zero can work on this.
		return false
	}

	cd.nextLevel = s.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	// Because this level and next level are both level 0, we should NOT acquire
	// the read lock twice, because it can result in a deadlock. So, we don't
	// call compactDef.lockLevels, instead locking the level only once and
	// directly here.
	//
	// As per godocs on RWMutex:
	// If a goroutine holds a RWMutex for reading and another goroutine might
	// call Lock, no goroutine should expect to be able to acquire a read lock
	// until the initial read lock is released. In particular, this prohibits
	// recursive read locking. This is to ensure that the lock eventually
	// becomes available; a blocked Lock call excludes new readers from
	// acquiring the lock.
	y.AssertTrue(cd.thisLevel.level == 0)
	y.AssertTrue(cd.nextLevel.level == 0)
	s.levels[0].RLock()
	defer s.levels[0].RUnlock()

	s.cstatus.Lock()
	defer s.cstatus.Unlock()

	top := cd.thisLevel.tables
	var out []*table.Table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// This file is already big, don't include it.
			continue
		}
		if now.Sub(t.CreatedAt) < 10*time.Second {
			// Just created it 10s ago. Don't pick for compaction.
			continue
		}
		if _, beingCompacted := s.cstatus.tables[t.ID()]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// If we don't have enough tables to merge in L0, don't do it.
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// Avoid any other L0 -> Lbase from happening, while this is going on.
	thisLevel := s.cstatus.levels[cd.thisLevel.level]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		s.cstatus.tables[t.ID()] = struct{}{}
	}

	// For L0->L0 compaction, we set the target file size to max, so the output is always one file.
	// This significantly decreases the L0 table stalls and improves the performance.
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

func (s *levelsController) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.level == 0 {
		panic("Base level can't be zero.")
	}
	// We keep cd.p.adjusted > 0.0 here to allow functions in db.go to artificially trigger
	// L0->Lbase compactions. Those functions wouldn't be setting the adjusted score.
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table.Table
	if len(cd.dropPrefixes) > 0 {
		// Use all tables if drop prefix is set. We don't want to compact only a
		// sub-range. We want to compact all the tables.
		out = top
	} else {
		var kr keyRange
		// cd.top[0] is the oldest file. So we start from the oldest file first.
		for _, t := range top {
			dkr := getKeyRange(t)
			if kr.overlapsWith(dkr) {
				out = append(out, t)
				kr.extend(dkr)
			} else {
				break
			}
		}
	}
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table.Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 would try to fill tables from L0 to be compacted with Lbase. If
// it can not do that, it would try to compact tables from L0 -> L0.
//
// Say L0 has 10 tables.
// fillTablesL0ToLbase picks up 5 tables to compact from L0 -> L5.
// Next call to fillTablesL0 would run L0ToLbase again, which fails this time.
// So, instead, we run fillTablesL0ToL0, which picks up rest of the 5 tables to
// be compacted within L0. Additionally, it would set the compaction range in
// cstatus to inf, so no other L0 -> Lbase compactions can happen.
// Thus, L0 -> L0 must finish for the next L0 -> Lbase to begin.
func (s *levelsController) fillTablesL0(cd *compactDef) bool {
	if ok := s.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return s.fillTablesL0ToL0(cd)
}

// sortByStaleData sorts tables based on the amount of stale data they have.
// This is useful in removing tombstones.
func (s *levelsController) sortByStaleDataSize(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// sortByHeuristic sorts tables in increasing order of MaxVersion, so we
// compact older tables first.
func (s *levelsController) sortByHeuristic(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].MaxVersion() < tables[j].MaxVersion()
	})
}

// This function should be called with lock on levels.
func (s *levelsController) fillMaxLevelTables(tables []*table.Table, cd *compactDef) bool {
	sortedTables := make([]*table.Table, len(tables))
	copy(sortedTables, tables)
	s.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table.Table{}
	collectBotTables := func(t *table.Table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return y.CompareKeys(tables[i].Smallest(), t.Smallest()) >= 0
		})
		y.AssertTrue(tables[j].ID() == t.ID())
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		// If the maxVersion is above the discardTs, we won't clean anything in
		// the compaction. So skip this table.
		if t.MaxVersion() > s.kv.orc.discardAtOrBelow() {
			continue
		}
		if now.Sub(t.CreatedAt) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}

		// Found a valid table!
		cd.top = []*table.Table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.level]
		// The table size is what we want so no need to collect more tables.
		if t.Size() >= needFileSz {
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz)
		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (s *levelsController) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table.Table, len(cd.thisLevel.tables))
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return s.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first.
	// This is similar to kOldestLargestSeqFirst in RocksDB.
	s.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// If we're already compacting this range, don't do anything.
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}
		cd.top = []*table.Table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
		cd.bot = make([]*table.Table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		// 下层没有 相关的 .sst 文件;
		if len(cd.bot) == 0 {
			cd.bot = []*table.Table{}
			cd.nextRange = cd.thisRange
			if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if s.cstatus.overlapsWith(cd.nextLevel.level, cd.nextRange) {
			continue
		}
		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

func (s *levelsController) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	y.AssertTrue(len(cd.splits) == 0)
	if thisLevel.level == nextLevel.level {
		// don't do anything for L0 -> L0 and Lmax -> Lmax.
	} else {
		s.addSplits(&cd)
	}

	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// Table should never be moved directly between levels,
	// always be rewritten to allow discarding invalid versions.

	newTables, decr, err := s.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()

	changeSet := buildChangeSet(&cd, newTables)

	// We write to the manifest _before_ we delete files (and after we created files)
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return err
	}

	getSizes := func(tables []*table.Table) int64 {
		size := int64(0)
		for _, i := range tables {
			size += i.Size()
		}
		return size
	}

	sizeNewTables := int64(0)
	sizeOldTables := int64(0)
	if s.kv.opt.MetricsEnabled {
		sizeNewTables = getSizes(newTables)
		sizeOldTables = getSizes(cd.bot) + getSizes(cd.top)
		y.NumBytesCompactionWrittenAdd(s.kv.opt.MetricsEnabled, nextLevel.strLevel, sizeNewTables)
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		s.kv.opt.Infof("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n, deleted %d bytes",
			id, expensive, thisLevel.level, nextLevel.level, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond), sizeOldTables-sizeNewTables)
	}

	if cd.thisLevel.level != 0 && len(newTables) > 2*s.kv.opt.LevelSizeMultiplier {
		s.kv.opt.Infof("This Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.top), hex.Dump(cd.thisRange.left), hex.Dump(cd.thisRange.right))
		s.kv.opt.Infof("Next Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.bot), hex.Dump(cd.nextRange.left), hex.Dump(cd.nextRange.right))
	}
	return nil
}

func tablesToString(tables []*table.Table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.ID()))
	}
	res = append(res, ".")
	return res
}

var errFillTables = stderrors.New("Unable to fill tables")

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(id int, p compactionPriority) error {
	l := p.level
	y.AssertTrue(l < s.kv.opt.MaxLevels) // Sanity check.
	if p.t.baseLevel == 0 {
		p.t = s.levelTargets()
	}

	_, span := otrace.StartSpan(context.Background(), "Badger.Compaction")
	defer span.End()

	cd := compactDef{
		compactorId:  id,
		span:         span,
		p:            p,
		t:            p.t,
		thisLevel:    s.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	if l == 0 {
		cd.nextLevel = s.levels[p.t.baseLevel]
		if !s.fillTablesL0(&cd) {
			return errFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// We're not compacting the last level so pick the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = s.levels[l+1]
		}
		if !s.fillTables(&cd) {
			return errFillTables
		}
	}
	defer s.cstatus.delete(cd) // Remove the ranges from compaction status.

	span.Annotatef(nil, "Compaction: %+v", cd)
	// 执行合并计划;
	if err := s.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		s.kv.opt.Warningf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	s.kv.opt.Debugf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.level)
	return nil
}

func (s *levelsController) addLevel0Table(t *table.Table) error {
	// Add table to manifest file only if it is not opened in memory. We don't want to add a table
	// to the manifest file if it exists only in memory.
	if !t.IsInmemory {
		// We update the manifest _before_ the table becomes part of a levelHandler, because at that
		// point it could get used in some compaction.  This ensures the manifest file gets updated in
		// the proper order. (That means this update happens before that of some compaction which
		// deletes the table.)
		err := s.kv.manifest.addChanges([]*pb.ManifestChange{
			newCreateChange(t.ID(), 0, t.KeyID(), t.CompressionType()),
		})
		if err != nil {
			return err
		}
	}

	// for 循环一直循环，直到添加成功，否则一直等待;
	for !s.levels[0].tryAddLevel0Table(t) {
		// Before we unstall, we need to make sure that level 0 is healthy.
		timeStart := time.Now()
		for s.levels[0].numTables() >= s.kv.opt.NumLevelZeroTablesStall {
			time.Sleep(10 * time.Millisecond)
		}
		dur := time.Since(timeStart)
		if dur > time.Second {
			s.kv.opt.Infof("L0 was stalled for %s\n", dur.Round(time.Millisecond))
		}
		s.l0stallsMs.Add(int64(dur.Round(time.Millisecond)))
	}
	return nil
}

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return y.Wrap(err, "levelsController.Close")
}

// get searches for a given key in all the levels of the LSM tree. It returns
// key version <= the expected version (version in key). If not found,
// it returns an empty y.ValueStruct.
func (s *levelsController) get(keyReadTs []byte, maxVs y.ValueStruct, startLevel int) (
	y.ValueStruct, error) {
	if s.kv.IsClosed() {
		return y.ValueStruct{}, ErrDBClosed
	}
	// It's important that we iterate the levels from 0 on upward. The reason is, if we iterated
	// in opposite order, or in parallel (naively calling all the h.RLock() in some order) we could
	// read level L's tables post-compaction and level L+1's tables pre-compaction.
	// (If we do parallelize this, we will need to call the h.RLock()
	// function by increasing order of level number.)
	readTsVersion := y.ParseTs(keyReadTs)
	for _, level := range s.levels {
		// Ignore all levels below startLevel. This is useful for GC when L0 is kept in memory.
		if level.level < startLevel {
			continue
		}
		// 在当前层中寻找, 有可能找到了,但是版本不对;
		levelVs, err := level.get(keyReadTs) // Calls h.RLock() and h.RUnlock().
		if err != nil {
			return y.ValueStruct{}, y.Wrapf(err, "get key: %q", keyReadTs)
		}
		if levelVs.Value == nil && levelVs.Meta == 0 {
			// 当前层没有找到, 继续下一层;
			continue
		}
		y.NumBytesReadsLSMAdd(s.kv.opt.MetricsEnabled, int64(len(levelVs.Value)))
		// 如果 .sst 找到的 == 原始的 readTs 版本;
		if levelVs.Version == readTsVersion {
			return levelVs, nil
		}
		// 都是不符合的情况:
		// levelVs.Version > readTsVersion  || levelVs.Version < readTsVersion
		// 根据查出来的数据版本号, 一直在更新 maxVs, 目的是为了最差情况, 有数据可返回;
		if levelVs.Version > maxVs.Version {
			maxVs = levelVs
		}
	} // for level over

	if len(maxVs.Value) > 0 {
		y.NumGetsWithResultsAdd(s.kv.opt.MetricsEnabled, 1)
	}

	// 否则到最后, 返回 存储的最高版本的数据;
	return maxVs, nil
}

func appendIteratorsReversed(out []y.Iterator, th []*table.Table, opt int) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

// appendIterators appends iterators to an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
func (s *levelsController) appendIterators(
	iters []y.Iterator, opt *IteratorOptions) []y.Iterator {
	// Just like with get, it's important we iterate the levels from 0 on upward, to avoid missing
	// data when there's a compaction.
	for _, level := range s.levels {
		iters = level.appendIterators(iters, opt)
	}
	return iters
}

// TableInfo represents the information about a table.
type TableInfo struct {
	ID               uint64
	Level            int
	Left             []byte
	Right            []byte
	KeyCount         uint32 // Number of keys in the table
	OnDiskSize       uint32
	StaleDataSize    uint32
	UncompressedSize uint32
	MaxVersion       uint64
	IndexSz          int
	BloomFilterSize  int
}

func (s *levelsController) getTableInfo() (result []TableInfo) {
	for _, l := range s.levels {
		l.RLock()
		for _, t := range l.tables {
			info := TableInfo{
				ID:               t.ID(),
				Level:            l.level,
				Left:             t.Smallest(),
				Right:            t.Biggest(),
				KeyCount:         t.KeyCount(),
				OnDiskSize:       t.OnDiskSize(),
				StaleDataSize:    t.StaleDataSize(),
				IndexSz:          t.IndexSize(),
				BloomFilterSize:  t.BloomFilterSize(),
				UncompressedSize: t.UncompressedSize(),
				MaxVersion:       t.MaxVersion(),
			}
			result = append(result, info)
		}
		l.RUnlock()
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].ID < result[j].ID
	})
	return
}

type LevelInfo struct {
	Level          int
	NumTables      int
	Size           int64
	TargetSize     int64
	TargetFileSize int64
	IsBaseLevel    bool
	Score          float64
	Adjusted       float64
	StaleDatSize   int64
}

func (s *levelsController) getLevelInfo() []LevelInfo {
	t := s.levelTargets()
	prios := s.pickCompactLevels(nil)
	result := make([]LevelInfo, len(s.levels))
	for i, l := range s.levels {
		l.RLock()
		result[i].Level = i
		result[i].Size = l.totalSize
		result[i].NumTables = len(l.tables)
		result[i].StaleDatSize = l.totalStaleSize

		l.RUnlock()

		result[i].TargetSize = t.targetSz[i]
		result[i].TargetFileSize = t.fileSz[i]
		result[i].IsBaseLevel = t.baseLevel == i
	}
	for _, p := range prios {
		result[p.level].Score = p.score
		result[p.level].Adjusted = p.adjusted
	}
	return result
}

// verifyChecksum verifies checksum for all tables on all levels.
func (s *levelsController) verifyChecksum() error {
	var tables []*table.Table
	for _, l := range s.levels {
		l.RLock()
		tables = tables[:0]
		for _, t := range l.tables {
			tables = append(tables, t)
			t.IncrRef()
		}
		l.RUnlock()

		for _, t := range tables {
			errChkVerify := t.VerifyChecksum()
			if err := t.DecrRef(); err != nil {
				s.kv.opt.Errorf("unable to decrease reference of table: %s while "+
					"verifying checksum with error: %s", t.Filename(), err)
			}

			if errChkVerify != nil {
				return errChkVerify
			}
		}
	}

	return nil
}

// Returns the sorted list of splits for all the levels and tables based
// on the block offsets.
func (s *levelsController) keySplits(numPerTable int, prefix []byte) []string {
	splits := make([]string, 0)
	for _, l := range s.levels {
		l.RLock()
		for _, t := range l.tables {
			tableSplits := t.KeySplits(numPerTable, prefix)
			splits = append(splits, tableSplits...)
		}
		l.RUnlock()
	}
	sort.Strings(splits)
	return splits
}
