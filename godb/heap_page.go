package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

// 一页的大小是固定的 PageSize 4096，还有一种页是 sortPage
type heapPage struct {
	// TODO: some code goes here

	// 页的头，共8字节，一个页里面只会存一种表
	totalSlots   int32 // 总的 slots 数目，也就是最大能存放的行数
	usedNumSlots int32 // 已使用的 slots 数目，已存放的行数

	tupleList []*Tuple   // 存储的行
	tupleDesc *TupleDesc // 列信息

	// 页的文件
	file *HeapFile
	// 页的编号，从0开始，用于file文件中定位位置
	pageNo int

	// 是否是脏页
	whetherDirty bool
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	bytesPerTuple := int64(0)
	for _, pojo := range desc.Fields {
		switch pojo.Ftype {
		case IntType:
			bytesPerTuple += int64(unsafe.Sizeof(int64(0)))
		case StringType:
			bytesPerTuple += ((int64)(unsafe.Sizeof(byte('a')))) * int64(StringLength)
		}
	}
	totalSlots := int32((int64(PageSize - 8)) / bytesPerTuple)
	return &heapPage{
		totalSlots:   totalSlots,
		usedNumSlots: 0,
		tupleList:    make([]*Tuple, totalSlots), // 直接初始化，每个元素默认为 nil
		tupleDesc:    desc,
		file:         f,
		pageNo:       pageNo,
	} //replace me
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	return int(h.totalSlots - h.usedNumSlots) //replace me
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	// 找到一个空闲的 slot
	for i, pojo := range h.tupleList {
		if pojo == nil {
			// 插入数据
			h.tupleList[i] = t
			h.usedNumSlots++
			t.Rid = RecordId{
				PageNo: h.pageNo,
				SlotNo: int32(i),
			}
			h.setDirty(true)
			return t.Rid, nil
		}
	}
	return RecordId{}, errors.New("insertTuple_err_no_free_slots") //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	val, ok := rid.(RecordId)
	if !ok {
		return fmt.Errorf("deleteTuple_rid_cast_error_%+v", rid)
	}
	if val.PageNo != h.pageNo {
		return fmt.Errorf("deleteTuple_rid_page_no_not_match_%+v", rid)
	}
	// 找到对应的 slot
	for i, pojo := range h.tupleList {
		if pojo != nil && val.SlotNo == int32(i) {
			h.tupleList[i] = nil
			h.usedNumSlots--
			h.setDirty(true)
			return nil
		}
	}
	return errors.New("deleteTuple_err_invalid_slot") //replace me

}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.whetherDirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	// TODO: some code goes here
	h.whetherDirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	// TODO: some code goes here
	var a DBFile
	a = p.file
	return &a //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	bs := &bytes.Buffer{}
	// 写入页头
	_ = binary.Write(bs, binary.LittleEndian, h.totalSlots)
	_ = binary.Write(bs, binary.LittleEndian, h.usedNumSlots)
	for _, pojo := range h.tupleList {
		// nil 值不序列化
		if pojo == nil {
			continue
		}
		_ = pojo.writeTo(bs)
	}
	// 如果一页不够 4096 , 最后需要填充一下
	paddingBytes := PageSize - bs.Len()
	_ = binary.Write(bs, binary.LittleEndian, make([]byte, paddingBytes))

	return bs, nil //replace me

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	if err := binary.Read(buf, binary.LittleEndian, &h.totalSlots); err != nil {
		panic(err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &h.usedNumSlots); err != nil {
		panic(err)
	}
	// 头中表示了有多少行
	for i := 0; i < int(h.usedNumSlots); i++ {
		from, err := readTupleFrom(buf, h.tupleDesc)
		if err != nil {
			return err
		}
		// rid 需要重新写入
		from.Rid = RecordId{
			PageNo: h.pageNo,
			SlotNo: int32(i),
		}
		h.tupleList[i] = from
	}
	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	// 迭代器需要用闭包实现。
	i := 0
	return func() (*Tuple, error) {
		for i < int(p.totalSlots) && p.tupleList[i] == nil {
			i++
		}
		// 找到第一个不为nil的
		if i < int(p.totalSlots) && p.tupleList[i] != nil {
			temp := p.tupleList[i]
			temp.Rid = RecordId{
				PageNo: p.pageNo,
				SlotNo: int32(i),
			}
			i++
			return temp, nil
		}
		return nil, nil
	}
}
