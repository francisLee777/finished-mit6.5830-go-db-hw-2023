package godb

// TODO: some code goes here
type InsertOp struct {
	// TODO: some code goes here
	insertFile DBFile
	child      Operator
	resDesc    *TupleDesc
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	desc := &TupleDesc{[]FieldType{{
		Fname: "count",
		Ftype: IntType,
	}}}
	return &InsertOp{
		resDesc:    desc,
		insertFile: insertFile,
		child:      child,
	}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return i.resDesc
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 把 child 中所有的元祖插入到 insertFile 中
	count := int64(0)
	iter, _ := iop.child.Iterator(tid)
	for t, _ := iter(); t != nil; t, _ = iter() {
		err := iop.insertFile.insertTuple(t, tid)
		if err != nil {
			return nil, err
		}
		count++
	}
	// 最后返回的是一个元祖，表示插入了多少行
	return func() (*Tuple, error) {
		temp := IntField{count}
		return &Tuple{
			Desc:   *iop.resDesc,
			Fields: []DBValue{temp},
		}, nil
	}, nil
}
