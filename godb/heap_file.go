package godb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
// 一个文件相当于一个表
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	// 表的描述
	td *TupleDesc
	// 表的文件名
	fileName string
	// 文件句柄
	fileFd *os.File
	// 表的缓存池和锁
	bufPool *BufferPool
	sync.Mutex
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// TODO: some code goes here
	open, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &HeapFile{
		td:       td,
		fileName: fromFile,
		bufPool:  bp,
		fileFd:   open,
		Mutex:    sync.Mutex{},
	}, nil //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	// 读取文件的大小 readme里面指出，用文件大小直接除以页大小
	fileInfo, err := f.fileFd.Stat()
	if err != nil {
		fmt.Println("文件打开失败")
		return 0
	}
	// 计算文件的页数
	return int(fileInfo.Size()) / PageSize
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
// readme 里面提到，这里是从文件中读取，而不是从 bufferPool 里面
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	// TODO: some code goes here
	pageBytes := make([]byte, PageSize)
	// 从固定的字节位置读取， 页号*页大小
	n, err := f.fileFd.ReadAt(pageBytes, int64(pageNo*PageSize))
	if err != nil {
		return nil, err
	}
	if n != PageSize {
		return nil, errors.New("HeapFile.readPage读取文件字节数不为4096")
	}
	// 读取出来之后要反序列化，先初始化一个 page
	page := newHeapPage(f.td, pageNo, f)
	if err = page.initFromBuffer(bytes.NewBuffer(pageBytes)); err != nil {
		return nil, err
	}
	var res Page
	res = page
	return &res, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	f.Lock()
	defer f.Unlock()
	// 文件中插入一个元祖，即表中插入一行，首先要找到对应的page进行插入，如果已有的所有page都没有位置可插入时，新生成页。
	// 首选要找到有空闲空间的page, 这里有多种寻找的策略，
	// 方案1. 循环整个表的所有页，根据页号从 buffer Pool 中取。 这种方案性能极差，因为数据的连续插入倾向于页号较大的页，导致页号靠前的页面中一般没有空闲位置。
	// 方案2. 优化方案1，循环整个表的所有页，但从页号最大的页开始循环。这种方案在实验二的bigJoin测试中有问题。在所有页的满的时候，for循环完整跑了所有页，并导致Buffer Pool把大量小页号的页载入，导致后续命中率极差，大量的读取请求需要去磁盘中读取，导致测试超时。
	// 方案3. 循环 buffer Pool 中已有的页面，寻找有空闲位置的页面。如果找不到，直接新建页面。在一些场景下会导致磁盘中出现页面空洞。
	// 综上，更高级的实现应该有一个地方 freePageList 专门记录哪些页面有空闲空间，参考 blot 的页面管理实现。
	f.bufPool.mu.Lock() // 防止 map 的并发读写问题，否则偶现 map fatal
	for tempPage := range f.bufPool.heapPageMap {
		f.bufPool.mu.Unlock() // 读完释放锁
		// buffer pool 里面可能有多个表文件的页面，需要对比一下
		if f.fileName != tempPage.FileName {
			f.bufPool.mu.Lock() // 下次循环加锁
			continue
		}
		// 从 bufferPool 里面找， 和底层文件的交互是 bufferPool 包掉的
		f.Unlock()
		page, err := f.bufPool.GetPage(f, tempPage.PageNo, tid, WritePerm)
		f.Lock()
		if err != nil {
			return err
		}
		// 看看 page 里面有空位置没有
		hp := (*page).(*heapPage)
		if hp.totalSlots-hp.usedNumSlots == 0 {
			f.bufPool.mu.Lock() // 下次循环加锁
			continue
		}
		// 有空位置，直接插入
		_, err = hp.insertTuple(t)
		if err != nil {
			return err
		}
		return nil
	}
	f.bufPool.mu.Unlock()
	//已有的所有page都没有位置可插入时，新生成页
	pageNo := f.NumPages() // 最后一个页号
	newPage := newHeapPage(f.td, pageNo, f)
	// 注意，这里不要插入 tuple 后再刷入磁盘，会导致没有提交的事务修改的页直接进入磁盘，不符 FORCE/NO STEAL 策略
	// 所以要先把新生成的空页刷到磁盘中，然后再用 Buffer Pool 把这个空页拿出来， 把 tuple 插入到空页中
	var page Page
	page = newPage
	if err := f.flushPage(&page); err != nil {
		return err
	}
	f.Unlock()
	pageFromPool, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
	f.Lock()
	if err != nil {
		return err
	}
	hp := (*pageFromPool).(*heapPage)
	_, err = hp.insertTuple(t)
	if err != nil {
		return err
	}
	return nil //replace me
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	f.Lock()
	defer f.Unlock()
	// TODO: some code goes here
	rid, ok := t.Rid.(RecordId)
	if !ok {
		return errors.New("deleteTuple_cast_RecordId_error")
	}
	f.Unlock()
	page, err := f.bufPool.GetPage(f, rid.PageNo, tid, WritePerm)
	f.Lock()
	if err != nil {
		return err
	}
	hp := (*page).(*heapPage)
	// 从 page 中删除, 设置脏
	err = hp.deleteTuple(rid)
	if err != nil {
		return err
	}
	return nil //replace me
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	// TODO: some code goes here
	// heapPage 的 toBuffer 保证了一定是 4096
	buffer, err := (*p).(*heapPage).toBuffer()
	if err != nil {
		return err
	}
	// 写入到固定的字节位置， 页号*页大小
	_, err = f.fileFd.WriteAt(buffer.Bytes(), int64((*p).(*heapPage).pageNo*PageSize))
	if err != nil {
		return err
	}
	// 不脏了
	(*p).(*heapPage).setDirty(false)
	return nil //replace me
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.td //replace me

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// 从一个表中迭代取行记录，每次要先迭代 page ， 再迭代里面的页slot
	// TODO: some code goes here
	i := 0
	var tempIter func() (*Tuple, error)
	return func() (*Tuple, error) {
		for i < f.NumPages() {
			pg, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
			if err != nil {
				return nil, err
			}
			hp := (*pg).(*heapPage)
			if tempIter == nil {
				tempIter = hp.tupleIter()
			}
			tuple, err := tempIter()
			if err != nil {
				return nil, err
			}
			if tuple != nil {
				return tuple, nil
			}
			// 本页面循环完了，重置内循环的 tempIter
			tempIter = nil
			i++
		}
		return nil, nil
	}, nil

}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	// TODO: some code goes here
	return heapHash{
		FileName: f.fileName,
		PageNo:   pgNo,
	}
}
