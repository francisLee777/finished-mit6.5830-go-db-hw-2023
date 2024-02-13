package godb

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	// TODO: some code goes here
	pageNumber  int                // 有多少页 page, 是 map的逻辑容量
	heapPageMap map[heapHash]*Page // 缓存的 page
	mu          sync.Mutex         // 保证 map 不会并发的锁

	// You will want to allocate data structures that keep track of the shared and exclusive locks each transaction is currently holding
	// 针对每一个事务，要维护他们所持有的锁list[对应页面list]
	// 实验中提到，锁的粒度是 page, 一个 page 上可以有多个事务读锁，一个事务会锁住多个页面，value 如果是 list 的话会有性能问题
	tidLockMap map[TransactionID]map[heapHash]pageLock

	// 为了死锁的检测和预防，需要维护一个锁等待队列, 某个tid 在等待哪些其他 tid . value 如果是 list 的话会有性能问题
	waitTidLockMap map[TransactionID]map[TransactionID]struct{}
}

// 实验中提到，锁的粒度是 page, 一个 page 上可以有多个读锁，一个锁也可以锁住多个页
type pageLock struct {
	page    *Page
	pageNo  int      // 从 page 里面也能拿到，但需要强转类型
	perm    RWPerm   // 读锁/写锁
	pageKey heapHash // Abort事务的时候，需要将页面从Buffer Pool中删除，需要获取页面Key
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
	// 初始化缓存
	bp := &BufferPool{
		pageNumber:     numPages,
		heapPageMap:    make(map[heapHash]*Page),
		tidLockMap:     make(map[TransactionID]map[heapHash]pageLock),
		waitTidLockMap: make(map[TransactionID]map[TransactionID]struct{}),
	}
	return bp
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	for _, page := range bp.heapPageMap {
		hp := (*page).(*heapPage)
		err := hp.file.flushPage(page)
		if err != nil {
			return
		}
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
// 因为NO STEAL策略，所以当缓存满了并且全都是脏页时，没法淘汰置换页面，所以需要放弃事务
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	// 把事务在 Buffer Pool 中修改的页面回滚掉，把 page 从 pool 中删除即可。
	for _, pgLock := range bp.tidLockMap[tid] {
		if pgLock.perm == WritePerm {
			delete(bp.heapPageMap, pgLock.pageKey)
		}
	}
	// 维护锁结构
	delete(bp.tidLockMap, tid)
	bp.deleteWaitTidLockMap(tid)
}

func (bp *BufferPool) abortTransactionWithoutLock(tid TransactionID) {
	// 把事务在 Buffer Pool 中修改的页面回滚掉，把 page 从 pool 中删除即可。
	for _, pgLock := range bp.tidLockMap[tid] {
		if pgLock.perm == WritePerm {
			//(*pgLock.page).setDirty(false)
			delete(bp.heapPageMap, pgLock.pageKey)
		}
	}
	// 维护锁结构
	delete(bp.tidLockMap, tid)
	bp.deleteWaitTidLockMap(tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	// 只提交本事务涉及的脏页
	for _, pgLock := range bp.tidLockMap[tid] {
		hp := (*pgLock.page).(*heapPage)
		if !hp.isDirty() {
			continue
		}
		hp.file.Lock()
		if err := hp.file.flushPage(pgLock.page); err != nil {
			panic(err)
		}
		hp.file.Unlock()
	}
	// 维护锁结构
	delete(bp.tidLockMap, tid)
	bp.deleteWaitTidLockMap(tid)
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	key := file.pageKey(pageNo).(heapHash)
	val, ok := bp.heapPageMap[key]
	if ok {
		// 处理事务相关
		if err := bp.handleTransactionInGetPage(pageNo, key, val, tid, perm); err != nil {
			return nil, err
		}
		return val, nil
	}
	// map 里面没有，要去底层存储读取了
	page, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}
	// 池子满了，要淘汰一个. 淘汰最后一个不脏的
	if len(bp.heapPageMap) >= bp.pageNumber {
		hasDelete := false
		// todo 这里先不实现淘汰算法，采取随机淘汰一个
		for _, page2 := range bp.heapPageMap {
			if !(*page2).isDirty() {
				hpPage := (*page2).(*heapPage)
				delete(bp.heapPageMap, hpPage.file.pageKey(hpPage.pageNo).(heapHash))
				hasDelete = true
				break
			}
		}
		if !hasDelete {
			// readme 如果缓冲池充满了脏页，则应该返回错误
			return nil, errors.New("buffer pool is full of dirty pages")
		}
	}
	bp.heapPageMap[key] = page
	// 处理事务相关
	if err = bp.handleTransactionInGetPage(pageNo, key, page, tid, perm); err != nil {
		return nil, err
	}
	return page, nil
}

func (bp *BufferPool) handleTransactionInGetPage(pageNo int, pageKey heapHash, page *Page, tid TransactionID, perm RWPerm) error {
	// 返回前需要判断事务锁等等
	// 如果页面上其他事务锁有冲突，那么通过 sleep 来等待
	for bp.pageLockedByOtherTid(pageNo, tid, perm) {
		// 是否存在死锁情况
		if bp.deadLockPrevent(tid, bp.waitTidLockMap[tid], map[TransactionID]bool{}) {
			// 需要放弃事务，放弃哪个事务有多种策略，这里就简单的放弃本事务
			bp.abortTransactionWithoutLock(tid)
			fmt.Println(pageNo, "页 ", *tid, "tid ", "发生死锁")
			time.Sleep(time.Duration(10+rand.Intn(10)) * time.Millisecond)
			// 这里需要返回 err，而不是继续，因为放弃了事务相当于整个 tid 涉及到的所有链路都失败了
			return errors.New("transaction_is_aborted")
		}
		// 放锁，让其他事务能够运行
		bp.mu.Unlock()
		time.Sleep(time.Duration(10+rand.Intn(10)) * time.Millisecond)
		bp.mu.Lock()
	}
	// 维护两个锁的状态
	// 这里注意：锁升级的问题。 当不存在或者入参是write时，才赋值。否则是大坑，很难排查
	if _, ok := bp.tidLockMap[tid][pageKey]; !ok || perm == WritePerm {
		if bp.tidLockMap[tid] == nil {
			bp.tidLockMap[tid] = map[heapHash]pageLock{}
		}
		// 注意：这里 page 对象必须用入参的 page ，不能用 bp.heapPageMap[key], 因为在并发场景下 bp 里面的页会被删除
		bp.tidLockMap[tid][pageKey] = pageLock{page: page, pageNo: pageNo, perm: perm, pageKey: pageKey}
	}
	// 等待map清除
	bp.deleteWaitTidLockMap(tid)
	return nil
}

// 某个页面是否被其他 tid 锁定.   tid 和 perm 是当前事务
func (bp *BufferPool) pageLockedByOtherTid(pageNo int, tid TransactionID, perm RWPerm) bool {
	found := false
	// 这里的性能不好，可以使用倒排索引去优化
	for t, pageLockMap := range bp.tidLockMap {
		if t == tid {
			continue
		}
		//fmt.Println("多循环了")
		for _, pgLock := range pageLockMap {
			if pgLock.pageNo == pageNo && (pgLock.perm == WritePerm || perm == WritePerm) {
				// 这里不直接 return 的原因是要写等待队列. 不要写入重复的
				if bp.waitTidLockMap[tid] == nil {
					bp.waitTidLockMap[tid] = map[TransactionID]struct{}{}
				}
				bp.waitTidLockMap[tid][t] = struct{}{}
				found = true
			}
		}
	}
	return found
}

// 死锁预防: 多种策略。这里仅通过 waitTidLockMap 判断，看下当前事务在等待的其他事务，有没有直接/间接等待自己[即循环等待]
// 涉及到递归检测，tid之间的等待关系是一个有向图，检测是否存在环, waitTidLockMap 相当于图的邻接表。
// 使用 dfs, 维护一个 visited 数组来标记是否已经遍历过
func (bp *BufferPool) deadLockPrevent(root TransactionID, tidMap map[TransactionID]struct{}, visitedMap map[TransactionID]bool) bool {
	// 遍历过程中，又回到了 root 节点，说明有环
	if _, ok := tidMap[root]; ok {
		return true
	}
	for tid := range tidMap {
		if visitedMap[tid] {
			continue
		}
		visitedMap[tid] = true
		if bp.deadLockPrevent(root, bp.waitTidLockMap[tid], visitedMap) {
			return true
		}
	}
	return false
}

func (bp *BufferPool) deleteWaitTidLockMap(tid TransactionID) {
	// 删除等待列表上，所有等待自己的
	for key, tidMap := range bp.waitTidLockMap {
		if tid == key {
			continue
		}
		delete(tidMap, tid)
		if len(tidMap) == 0 {
			delete(bp.waitTidLockMap, key)
		}
	}
	// 删除自己的等待列表
	delete(bp.waitTidLockMap, tid)
}
