package godb

import (
	"errors"
	"github.com/mitchellh/hashstructure/v2"
)

// Project 投影，针对某个Tuple集合，仅选取指定的列 例如:select name,age from person
type Project struct {
	selectFields []Expr   // required fields for parser
	outputNames  []string // 相当于 select name as n 中的 as 重新命名
	child        Operator
	//add additional fields here
	// TODO: some code goes here
	distinct bool
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	if len(selectFields) != len(outputNames) {
		return nil, errors.New("NewProjectOp_selectFields_outputNames_len_not_equal")
	}
	return &Project{
		selectFields: selectFields,
		outputNames:  outputNames,
		child:        child,
		distinct:     distinct,
	}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	res := &TupleDesc{}
	// 这里注意，要把 alias 注入进去, 否则投影会失败
	for i, pojo := range p.selectFields {
		temp := pojo.GetExprType()
		temp.Fname = p.outputNames[i]
		res.Fields = append(res.Fields, temp)
	}
	return res
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 在 select distinct 的情况下要记录一下当前所访问过的值
	distinctMap := make(map[uint64]int)
	iterator, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for tuple, _ := iterator(); tuple != nil; tuple, _ = iterator() {
			tupleHash, err := hashTupleBySelectFields(tuple, p.selectFields)
			if err != nil {
				return nil, err
			}
			if p.distinct && distinctMap[tupleHash] != 0 {
				continue
			}
			distinctMap[tupleHash] = 1
			temp := make([]FieldType, 0)
			for _, pojo := range p.selectFields {
				temp = append(temp, pojo.GetExprType())
			}
			projectRes, err := tuple.project(temp)
			if err != nil {
				return nil, err
			}
			projectRes.Desc = *p.Descriptor()
			return projectRes, nil
		}
		return nil, nil
	}, nil
}

// 根据Tuple中某几个列进行hash
func hashTupleBySelectFields(tuple *Tuple, selectFields []Expr) (uint64, error) {
	selectValueList := make([]DBValue, 0)
	for _, pojo := range selectFields {
		expr, err := pojo.EvalExpr(tuple)
		if err != nil {
			return 0, nil
		}
		selectValueList = append(selectValueList, expr)
	}
	return hashstructure.Hash(selectValueList, hashstructure.FormatV2, nil)
}
