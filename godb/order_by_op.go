package godb

// TODO: some code goes here
type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	//add additional fields here
	ascendingList []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	return &OrderBy{
		orderBy:       orderByFields,
		child:         child,
		ascendingList: ascending,
	}, nil //replace me

}

func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor().copy()
	// 思考一下为什么不是 orderBy 的 Descriptor
	//res := &TupleDesc{}
	//for _, pojo := range o.orderBy {
	//	res.Fields = append(res.Fields, pojo.GetExprType())
	//}
	//return res
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 排序是阻塞的，意味着和其他操作的迭代器不同，需要在内存中对所有结果处理完才能返回迭代器，而非逐个处理逐个返回。
	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	res := make([]*Tuple, 0)
	for tuple, _ := iter(); tuple != nil; tuple, _ = iter() {
		res = append(res, tuple)
	}
	// 进行内存排序
	lessFuncList := make([]LessFunc, 0)
	for i, pojo := range o.orderBy {
		// 注意这里，由于闭包的特性，需要把 for 中的临时变量抽出来重新赋值一下
		tempI := i
		tempPojo := pojo
		lessFuncList = append(lessFuncList, func(p1, p2 *Tuple) bool {
			order, _ := p1.compareField(p2, tempPojo)
			// 需要看下是升序还是降序
			if o.ascendingList[tempI] {
				return order < OrderedEqual
			}
			return order > OrderedEqual
		})
	}
	OrderedBy(lessFuncList...).Sort(res)
	// 返回迭代器
	i := 0
	return func() (*Tuple, error) {
		if i >= len(res) {
			return nil, nil
		}
		i++
		return res[i-1], nil
	}, nil
}
