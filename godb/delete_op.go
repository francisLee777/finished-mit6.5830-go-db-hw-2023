package godb

type DeleteOp struct {
	// TODO: some code goes here
	deleteFile DBFile
	child      Operator
	resDesc    *TupleDesc
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	desc := &TupleDesc{[]FieldType{{
		Fname: "count",
		Ftype: IntType,
	}}}
	return &DeleteOp{
		resDesc:    desc,
		deleteFile: deleteFile,
		child:      child,
	}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return i.resDesc

}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 把 child 中所有的元祖从 insertFile 中删除
	count := int64(0)
	iter, _ := dop.child.Iterator(tid)
	for t, _ := iter(); t != nil; t, _ = iter() {
		err := dop.deleteFile.deleteTuple(t, tid)
		if err != nil {
			return nil, err
		}
		count++
	}
	// 最后返回的是一个元祖，表示插入了多少行
	return func() (*Tuple, error) {
		temp := IntField{count}
		return &Tuple{
			Desc:   *dop.resDesc,
			Fields: []DBValue{temp},
		}, nil
	}, nil

}
