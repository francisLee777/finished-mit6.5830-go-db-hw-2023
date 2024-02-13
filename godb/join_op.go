package godb

type EqualityJoin[T comparable] struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator //operators for the two inputs of the join

	// Function that when applied to a DBValue returns the join value; will be
	// one of intFilterGetter or stringFilterGetter
	getter func(DBValue) T

	// The maximum number of records of intermediate state that the join should use
	// (only required for optional exercise)
	maxBufferSize int
}

// Constructor for a  join of integer expressions
// Returns an error if either the left or right expression is not an integer
func NewIntJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[int64], error) {
	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return nil, GoDBError{TypeMismatchError, "join field is not an int"}
	case IntType:
		return &EqualityJoin[int64]{leftField, rightField, &left, &right, intFilterGetter, maxBufferSize}, nil
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Constructor for a  join of string expressions
// Returns an error if either the left or right expression is not a string
func NewStringJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[string], error) {

	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return &EqualityJoin[string]{leftField, rightField, &left, &right, stringFilterGetter, maxBufferSize}, nil
	case IntType:
		return nil, GoDBError{TypeMismatchError, "join field is not a string"}
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Return a TupleDescriptor for this join. The returned descriptor should contain
// the union of the fields in the descriptors of the left and right operators.
// HINT: use the merge function you implemented for TupleDesc in lab1
func (hj *EqualityJoin[T]) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// 根据注释，返回的是左右两边的 desc 组合, 借助之前实现的 TupleDesc 的 merge 方法
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())
}

// Join operator implementation.  This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
// HINT: When implementing the simple nested loop join, you should keep in mind that
// you only iterate through the left iterator once (outer loop) but iterate through the right iterator
// once for every tuple in the the left iterator (inner loop).
// HINT: You can use joinTuples function you implemented in lab1 to join two tuples.
//
// OPTIONAL EXERCISE:  the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out.  To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin[T]) Iterator1(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter1, _ := (*joinOp.left).Iterator(tid)
	var iter2 func() (*Tuple, error)
	var tuple1 *Tuple
	return func() (*Tuple, error) {
		for {
			// 代表内层循环完毕，内层迭代器需要重置，且外层迭代器需要迭代一次
			if iter2 == nil {
				tuple1, _ = iter1()
				if tuple1 == nil {
					return nil, nil // 外层迭代完毕，整体终止
				}
				iter2, _ = (*joinOp.right).Iterator(tid)
			}
			v1, _ := joinOp.leftField.EvalExpr(tuple1)
			// 进行内层迭代
			for tuple2, _ := iter2(); tuple2 != nil; tuple2, _ = iter2() {
				v2, err := joinOp.rightField.EvalExpr(tuple2)
				if err != nil {
					return nil, err
				}
				// 这里因为 EqualityJoin 结构体定义中的泛型是 comparable 而不是 constraints.Ordered 所以没法用 evalPred 函数进行比较
				// go1.18后的泛型，comparable的意思是可以直接使用 == 号进行判断
				tempV1 := (*joinOp).getter(v1)
				tempV2 := (*joinOp).getter(v2)
				if tempV1 == tempV2 {
					return joinTuples(tuple1, tuple2), nil
				}
			}
			// 代表内层迭代完毕，内层迭代器需要重置
			iter2 = nil
		}
	}, nil
}

func (joinOp *EqualityJoin[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 使用 Sort-Merge Join 方法，对join列进行排序, 时间复杂度为 O(m+n), 但有空间复杂度 O(m+n).
	// 如果join列有索引的话，那么相当于已经有序，直接迭代器足够，不需要在内存中做排序。
	// 忽略 maxBufferSize 参数要求，直接拖到内存里排序
	// 如果考虑空间复杂度不超过 maxBufferSize 的要求，则需要采用 Hash Join 方法，
	// 为其中一张表（通常是较小的表，称为 "build" 表）创建一个在内存中的临时哈希表，驱动另外一个表做循环匹配。
	// 需要注意的是解决 hash 冲突问题。时间复杂度约等于 O(m/size * n) 空间复杂度为 O(size)
	tupleList1, tupleList2, resList := make([]*Tuple, 0), make([]*Tuple, 0), make([]*Tuple, 0)
	iter1, _ := (*joinOp.left).Iterator(tid)
	iter2, _ := (*joinOp.right).Iterator(tid)
	for tuple1, _ := iter1(); tuple1 != nil; tuple1, _ = iter1() {
		tupleList1 = append(tupleList1, tuple1)
	}
	for tuple2, _ := iter2(); tuple2 != nil; tuple2, _ = iter2() {
		tupleList2 = append(tupleList2, tuple2)
	}
	sortTupleList(tupleList1, joinOp.leftField)
	sortTupleList(tupleList2, joinOp.rightField)
	// 双指针法
	index1, index2 := 0, 0
	for index1 < len(tupleList1) && index2 < len(tupleList2) {
		// 可能是两个不同的列之间进行比较，需要自己写一个 compareField 函数
		order, err := compareField(tupleList1[index1], tupleList2[index2], joinOp.leftField, joinOp.rightField)
		if err != nil {
			return nil, err
		}
		switch order {
		case OrderedEqual:
			// 算法题：  找出数组 [1,2,2,4]  和 [0,2,2,4,4] 的相等元素对，有6组 [2,2] [2,2] [2,2] [2,2] [4,4] [4,4]
			// 要考虑重复相等的情况
			end1, end2 := index1+1, index2+1
			for end1 < len(tupleList1) {
				val, _ := tupleList1[end1].compareField(tupleList1[index1], joinOp.leftField)
				if val == OrderedEqual {
					end1++
				} else {
					break
				}
			}
			for end2 < len(tupleList2) {
				val, _ := tupleList2[end2].compareField(tupleList2[index2], joinOp.rightField)
				if val == OrderedEqual {
					end2++
				} else {
					break
				}
			}
			for i := index1; i < end1; i++ {
				for j := index2; j < end2; j++ {
					resList = append(resList, joinTuples(tupleList1[i], tupleList2[j]))
				}
			}
			index1 = end1
			index2 = end2
		case OrderedLessThan:
			index1++
		case OrderedGreaterThan:
			index2++
		}
	}
	index := 0
	return func() (*Tuple, error) {
		if index >= len(resList) {
			return nil, nil
		}
		index++
		return resList[index-1], nil
	}, nil
}
