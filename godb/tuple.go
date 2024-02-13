package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	// TODO: some code goes here
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}
	for i, f1 := range d1.Fields {
		f2 := d2.Fields[i]
		if int(f1.Ftype) != int(f2.Ftype) || f1.Fname != f2.Fname || f1.TableQualifier != f2.TableQualifier {
			return false
		}
	}
	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	tempList := make([]FieldType, len(td.Fields))
	copy(tempList, td.Fields)
	return &TupleDesc{Fields: tempList} //replace me
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO: some code goes here
	//create a new tuple desc
	newFields := make([]FieldType, len(desc.Fields)+len(desc2.Fields))
	copy(newFields, desc.Fields)
	copy(newFields[len(desc.Fields):], desc2.Fields)
	return &TupleDesc{Fields: newFields} //replace me
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

func (i1 IntField) compare(i2 IntField) orderByState {
	if i1.Value < i2.Value {
		return OrderedLessThan
	}
	if i1.Value > i2.Value {
		return OrderedGreaterThan
	}
	return OrderedEqual
}

// String field value
type StringField struct {
	Value string
}

// string 的 compare 可以直接用 < > 号吗
func (i1 StringField) compare(i2 StringField) orderByState {
	if i1.Value < i2.Value {
		return OrderedLessThan
	}
	if i1.Value > i2.Value {
		return OrderedGreaterThan
	}
	return OrderedEqual
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc // 理论上说， desc 是对 fields 每个元素的描述，例如数据的列名和数据的类型
	Fields []DBValue // 仅仅是数据的值
	Rid    recordID  //used to track the page and position this page was read from
}

type recordID interface {
}

// RecordId 实现 used to track the page and position this page was read from
type RecordId struct {
	PageNo int
	SlotNo int32 // 从0开始
}

// MultiSorter 用于排序, 模仿 https://pkg.go.dev/sort 中的 SortMultiKeys 实现 sort.Sort 接口
type MultiSorter struct {
	tuples  []*Tuple
	lessFun []LessFunc
}

type LessFunc func(p1, p2 *Tuple) bool

func (ms *MultiSorter) Sort(changes []*Tuple) {
	ms.tuples = changes
	sort.Sort(ms)
}

func OrderedBy(less ...LessFunc) *MultiSorter {
	return &MultiSorter{
		lessFun: less,
	}
}

func (ms *MultiSorter) Len() int {
	return len(ms.tuples)
}

func (ms *MultiSorter) Swap(i, j int) {
	ms.tuples[i], ms.tuples[j] = ms.tuples[j], ms.tuples[i]
}

func (ms *MultiSorter) Less(i, j int) bool {
	p, q := ms.tuples[i], ms.tuples[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.lessFun)-1; k++ {
		less := ms.lessFun[k]
		switch {
		case less(p, q):
			// p < q, so we have a decision.
			return true
		case less(q, p):
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	return ms.lessFun[k](p, q)
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO: some code goes here
	// 接口要求 Data must be a fixed-size value or a slice of fixed-size 所以不能使用简单的 json.Marshal 这种序列化方式
	// 否则，每次序列化得到的字节数不固定，导致后面根据页面大小4096和记录号id，无法确定一个记录的字节位置
	// 只需要序列化 value 即可，列信息不需要
	for _, pojo := range t.Fields {
		switch pojo.(type) {
		case IntField:
			if err := binary.Write(b, binary.LittleEndian, pojo.(IntField).Value); err != nil {
				return err
			}
		case StringField:
			temp := []byte(pojo.(StringField).Value)
			// str 是定长，多了截断，少了补齐
			if len(temp) > StringLength {
				temp = temp[0:StringLength]
			} else {
				padding := make([]byte, StringLength-len(temp))
				temp = append(temp, padding...)
			}
			if err := binary.Write(b, binary.LittleEndian, temp); err != nil {
				return err
			}
		}
	}
	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	res := &Tuple{}
	res.Desc = *desc
	res.Fields = make([]DBValue, len(desc.Fields))
	for i := 0; i < len(desc.Fields); i++ {
		switch desc.Fields[i].Ftype {
		case IntType:
			var temp IntField
			if err := binary.Read(b, binary.LittleEndian, &temp.Value); err != nil {
				return nil, err
			}
			res.Fields[i] = temp
		case StringType:
			temp := make([]byte, StringLength)
			if err := binary.Read(b, binary.LittleEndian, &temp); err != nil {
				return nil, err
			}
			// 只取temp中有效的字节
			res.Fields[i] = StringField{strings.TrimRight(string(temp), "\x00")}
		}
	}
	return res, nil //replace me
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	if t1 == nil && t2 != nil || t1 != nil && t2 == nil {
		return false
	}
	if t1.Desc.equals(&t2.Desc) == false {
		return false
	}
	if len(t1.Fields) != len(t2.Fields) {
		return false
	}
	for i, f := range t1.Fields {
		// todo interface类型的数据可以直接用=号吗
		if f != t2.Fields[i] {
			return false
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here
	res := &Tuple{}
	if t1 != nil {
		res.Desc = t1.Desc
		res.Fields = t1.Fields
		res.Rid = t1.Rid
	}
	if t2 != nil {
		res.Desc.Fields = append(res.Desc.Fields, t2.Desc.Fields...)
		res.Fields = append(res.Fields, t2.Fields...)
	}
	return res
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
// 根据 运算符比较 t1 和 t2 两个元素的大小，用于排序
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	expr1, err := field.EvalExpr(t)
	if err != nil {
		return 0, err
	}
	expr2, err := field.EvalExpr(t2)
	if err != nil {
		return 0, err
	}
	// expr1 和 expr2 的类型应该一样
	switch expr1.(type) {
	case IntField:
		return expr1.(IntField).compare(expr2.(IntField)), nil
	case StringField:
		return expr1.(StringField).compare(expr2.(StringField)), nil
	}
	return OrderedEqual, nil // replace me
}

func compareField(t1, t2 *Tuple, f1, f2 Expr) (orderByState, error) {
	expr1, err := f1.EvalExpr(t1)
	if err != nil {
		return 0, err
	}
	expr2, err := f2.EvalExpr(t2)
	if err != nil {
		return 0, err
	}
	// expr1 和 expr2 的类型应该一样
	switch expr1.(type) {
	case IntField:
		return expr1.(IntField).compare(expr2.(IntField)), nil
	case StringField:
		return expr1.(StringField).compare(expr2.(StringField)), nil
	}
	return OrderedEqual, nil
}

// 指定某一列，进行行排序。 从小到大
func sortTupleList(list []*Tuple, field Expr) {
	sort.Slice(list, func(i, j int) bool {
		tempRes, err := list[i].compareField(list[j], field)
		if err != nil {
			return false
		}
		return tempRes < OrderedEqual
	})
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
// 根据 fields 把 t 中的匹配字段挑出来形成新的 tuple
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here
	if t == nil || len(fields) == 0 {
		return nil, nil
	}
	res := &Tuple{}
	res.Rid = t.Rid
	for _, pojo1 := range fields {
		index, _ := findFieldInTd(pojo1, &t.Desc)
		if index != -1 {
			res.Desc.Fields = append(res.Desc.Fields, t.Desc.Fields[index])
			res.Fields = append(res.Fields, t.Fields[index])
		}
	}
	return res, nil //replace me
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
