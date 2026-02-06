package reflectx

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ival(v reflect.Value) int {
	return v.Interface().(int)
}

func TestBasic(t *testing.T) {
	type Foo struct {
		A int
		B int
		C int
	}

	f := Foo{1, 2, 3}
	fv := reflect.ValueOf(f)
	m := NewMapperFunc("", func(s string) string { return s })

	v := m.FieldByName(fv, "A")
	assert.Equal(t, f.A, ival(v))
	v = m.FieldByName(fv, "B")
	assert.Equal(t, f.B, ival(v))
	v = m.FieldByName(fv, "C")
	assert.Equal(t, f.C, ival(v))
}

func TestBasicEmbedded(t *testing.T) {
	type Foo struct {
		A int
	}

	type Bar struct {
		Foo // `db:""` is implied for an embedded struct
		B   int
		C   int `db:"-"`
	}

	type Baz struct {
		A   int
		Bar `db:"Bar"`
	}

	m := NewMapperFunc("db", func(s string) string { return s })

	z := Baz{}
	z.A = 1
	z.B = 2
	z.C = 4
	z.Foo.A = 3

	zv := reflect.ValueOf(z)
	fields := m.TypeMap(reflect.TypeOf(z))

	assert.Len(t, fields.Index, 5)

	// for _, fi := range fields.Index {
	// 	log.Println(fi)
	// }

	v := m.FieldByName(zv, "A")
	assert.Equal(t, z.A, ival(v))
	v = m.FieldByName(zv, "Bar.B")
	assert.Equal(t, z.B, ival(v))
	v = m.FieldByName(zv, "Bar.A")
	assert.Equal(t, z.Foo.A, ival(v))
	v = m.FieldByName(zv, "Bar.C")
	_, ok := v.Interface().(int)
	assert.False(t, ok, "Expecting Bar.C to not exist")

	fi := fields.GetByPath("Bar.C")
	assert.Nil(t, fi, "Bar.C should not exist")
}

func TestEmbeddedSimple(t *testing.T) {
	type UUID [16]byte
	type MyID struct {
		UUID
	}
	type Item struct {
		ID MyID
	}
	z := Item{}

	m := NewMapper("db")
	m.TypeMap(reflect.TypeOf(z))
}

func TestBasicEmbeddedWithTags(t *testing.T) {
	type Foo struct {
		A int `db:"a"`
	}

	type Bar struct {
		Foo     // `db:""` is implied for an embedded struct
		B   int `db:"b"`
	}

	type Baz struct {
		A   int `db:"a"`
		Bar     // `db:""` is implied for an embedded struct
	}

	m := NewMapper("db")

	z := Baz{}
	z.A = 1
	z.B = 2
	z.Foo.A = 3

	zv := reflect.ValueOf(z)
	fields := m.TypeMap(reflect.TypeOf(z))

	assert.Len(t, fields.Index, 5)

	// for _, fi := range fields.index {
	// 	log.Println(fi)
	// }

	v := m.FieldByName(zv, "a")
	assert.Equal(t, z.A, ival(v)) // the dominant field
	v = m.FieldByName(zv, "b")
	assert.Equal(t, z.B, ival(v))
}

func TestBasicEmbeddedWithSameName(t *testing.T) {
	type Foo struct {
		A   int `db:"a"`
		Foo int `db:"Foo"` // Same name as the embedded struct
	}

	type FooExt struct {
		Foo
		B int `db:"b"`
	}

	m := NewMapper("db")

	z := FooExt{}
	z.A = 1
	z.B = 2
	z.Foo.Foo = 3

	zv := reflect.ValueOf(z)
	fields := m.TypeMap(reflect.TypeOf(z))

	assert.Len(t, fields.Index, 4)

	v := m.FieldByName(zv, "a")
	assert.Equal(t, z.A, ival(v)) // the dominant field
	v = m.FieldByName(zv, "b")
	assert.Equal(t, z.B, ival(v))
	v = m.FieldByName(zv, "Foo")
	assert.Equal(t, z.Foo.Foo, ival(v))
}

func TestFlatTags(t *testing.T) {
	m := NewMapper("db")

	type Asset struct {
		Title string `db:"title"`
	}
	type Post struct {
		Author string `db:"author,required"`
		Asset  Asset  `db:""`
	}
	// Post columns: (author title)

	post := Post{Author: "Joe", Asset: Asset{Title: "Hello"}}
	pv := reflect.ValueOf(post)

	v := m.FieldByName(pv, "author")
	assert.Equal(t, post.Author, v.Interface().(string))
	v = m.FieldByName(pv, "title")
	assert.Equal(t, post.Asset.Title, v.Interface().(string))
}

func TestNestedStruct(t *testing.T) {
	m := NewMapper("db")

	type Details struct {
		Active bool `db:"active"`
	}
	type Asset struct {
		Title   string  `db:"title"`
		Details Details `db:"details"`
	}
	type Post struct {
		Author string `db:"author,required"`
		Asset  `db:"asset"`
	}
	// Post columns: (author asset.title asset.details.active)

	post := Post{
		Author: "Joe",
		Asset:  Asset{Title: "Hello", Details: Details{Active: true}},
	}
	pv := reflect.ValueOf(post)

	v := m.FieldByName(pv, "author")
	assert.Equal(t, post.Author, v.Interface().(string))
	v = m.FieldByName(pv, "title")
	_, ok := v.Interface().(string)
	assert.False(t, ok, "Expecting field to not exist")
	v = m.FieldByName(pv, "asset.title")
	assert.Equal(t, post.Title, v.Interface().(string))
	v = m.FieldByName(pv, "asset.details.active")
	assert.Equal(t, post.Details.Active, v.Interface().(bool))
}

func TestInlineStruct(t *testing.T) {
	m := NewMapperTagFunc("db", strings.ToLower, nil)

	type Employee struct {
		Name string
		ID   int
	}
	type Boss Employee
	type person struct {
		Employee `db:"employee"`
		Boss     `db:"boss"`
	}
	// employees columns: (employee.name employee.id boss.name boss.id)

	em := person{Employee: Employee{Name: "Joe", ID: 2}, Boss: Boss{Name: "Dick", ID: 1}}
	ev := reflect.ValueOf(em)

	fields := m.TypeMap(reflect.TypeOf(em))
	assert.Len(t, fields.Index, 6)

	v := m.FieldByName(ev, "employee.name")
	assert.Equal(t, em.Employee.Name, v.Interface().(string))
	v = m.FieldByName(ev, "boss.id")
	assert.Equal(t, em.Boss.ID, ival(v))
}

func TestRecursiveStruct(t *testing.T) {
	type Person struct {
		Parent *Person
	}
	m := NewMapperFunc("db", strings.ToLower)
	var p *Person
	m.TypeMap(reflect.TypeOf(p))
}

func TestFieldsEmbedded(t *testing.T) {
	m := NewMapper("db")

	type Person struct {
		Name string `db:"name,size=64"`
	}
	type Place struct {
		Name string `db:"name"`
	}
	type Article struct {
		Title string `db:"title"`
	}
	type PP struct {
		Person  `db:"person,required"`
		Place   `db:",someflag"`
		Article `db:",required"`
	}
	// PP columns: (person.name name title)

	pp := PP{}
	pp.Person.Name = "Peter"
	pp.Place.Name = "Toronto"
	pp.Title = "Best city ever"

	fields := m.TypeMap(reflect.TypeOf(pp))
	// for i, f := range fields {
	// 	log.Println(i, f)
	// }

	ppv := reflect.ValueOf(pp)

	v := m.FieldByName(ppv, "person.name")
	assert.Equal(t, pp.Person.Name, v.Interface().(string))

	v = m.FieldByName(ppv, "name")
	assert.Equal(t, pp.Place.Name, v.Interface().(string))

	v = m.FieldByName(ppv, "title")
	assert.Equal(t, pp.Title, v.Interface().(string))

	fi := fields.GetByPath("person")
	_, ok := fi.Options["required"]
	assert.True(t, ok, "Expecting required option to be set")
	assert.True(t, fi.Embedded, "Expecting field to be embedded")
	assert.Equal(t, []int{0}, fi.Index)

	fi = fields.GetByPath("person.name")
	require.NotNil(t, fi, "Expecting person.name to exist")
	assert.Equal(t, "person.name", fi.Path)
	assert.Equal(t, "64", fi.Options["size"])

	fi = fields.GetByTraversal([]int{1, 0})
	require.NotNil(t, fi, "Expecting traversal to exist")
	assert.Equal(t, "name", fi.Path)

	fi = fields.GetByTraversal([]int{2})
	require.NotNil(t, fi, "Expecting traversal to exist")
	_, ok = fi.Options["required"]
	assert.True(t, ok, "Expecting required option to be set")

	trs := m.TraversalsByName(reflect.TypeOf(pp), []string{"person.name", "name", "title"})
	assert.Equal(t, [][]int{{0, 0}, {1, 0}, {2, 0}}, trs)
}

func TestPtrFields(t *testing.T) {
	m := NewMapperTagFunc("db", strings.ToLower, nil)
	type Asset struct {
		Title string
	}
	type Post struct {
		*Asset `db:"asset"`
		Author string
	}

	post := &Post{Author: "Joe", Asset: &Asset{Title: "Hiyo"}}
	pv := reflect.ValueOf(post)

	fields := m.TypeMap(reflect.TypeOf(post))
	assert.Len(t, fields.Index, 3)

	v := m.FieldByName(pv, "asset.title")
	assert.Equal(t, post.Title, v.Interface().(string))
	v = m.FieldByName(pv, "author")
	assert.Equal(t, post.Author, v.Interface().(string))
}

func TestNamedPtrFields(t *testing.T) {
	m := NewMapperTagFunc("db", strings.ToLower, nil)

	type User struct {
		Name string
	}

	type Asset struct {
		Title string

		Owner *User `db:"owner"`
	}
	type Post struct {
		Author string

		Asset1 *Asset `db:"asset1"`
		Asset2 *Asset `db:"asset2"`
	}

	post := &Post{Author: "Joe", Asset1: &Asset{Title: "Hiyo", Owner: &User{"Username"}}} // Let Asset2 be nil
	pv := reflect.ValueOf(post)

	fields := m.TypeMap(reflect.TypeOf(post))
	assert.Len(t, fields.Index, 9)

	v := m.FieldByName(pv, "asset1.title")
	assert.Equal(t, post.Asset1.Title, v.Interface().(string))
	v = m.FieldByName(pv, "asset1.owner.name")
	assert.Equal(t, post.Asset1.Owner.Name, v.Interface().(string))
	v = m.FieldByName(pv, "asset2.title")
	assert.Equal(t, post.Asset2.Title, v.Interface().(string))
	v = m.FieldByName(pv, "asset2.owner.name")
	assert.Equal(t, post.Asset2.Owner.Name, v.Interface().(string))
	v = m.FieldByName(pv, "author")
	assert.Equal(t, post.Author, v.Interface().(string))
}

func TestFieldMap(t *testing.T) {
	type Foo struct {
		A int
		B int
		C int
	}

	f := Foo{1, 2, 3}
	m := NewMapperFunc("db", strings.ToLower)

	fm := m.FieldMap(reflect.ValueOf(f))

	assert.Len(t, fm, 3)
	assert.Equal(t, 1, fm["a"].Interface().(int))
	assert.Equal(t, 2, fm["b"].Interface().(int))
	assert.Equal(t, 3, fm["c"].Interface().(int))
}

func TestTagNameMapping(t *testing.T) {
	type Strategy struct {
		StrategyID   string `protobuf:"bytes,1,opt,name=strategy_id" json:"strategy_id,omitempty"`
		StrategyName string
	}

	m := NewMapperTagFunc("json", strings.ToUpper, func(value string) string {
		if strings.Contains(value, ",") {
			return strings.Split(value, ",")[0]
		}
		return value
	})
	strategy := Strategy{"1", "Alpah"}
	mapping := m.TypeMap(reflect.TypeOf(strategy))

	for _, key := range []string{"strategy_id", "STRATEGYNAME"} {
		fi := mapping.GetByPath(key)
		assert.NotNil(t, fi, "Expecting to find key %s in mapping", key)
	}
}

func TestMapping(t *testing.T) {
	type Person struct {
		ID           int
		Name         string
		WearsGlasses bool `db:"wears_glasses"`
	}

	m := NewMapperFunc("db", strings.ToLower)
	p := Person{1, "Jason", true}
	mapping := m.TypeMap(reflect.TypeOf(p))

	for _, key := range []string{"id", "name", "wears_glasses"} {
		fi := mapping.GetByPath(key)
		assert.NotNil(t, fi, "Expecting to find key %s in mapping", key)
	}

	type SportsPerson struct {
		Weight int
		Age    int
		Person
	}
	s := SportsPerson{Weight: 100, Age: 30, Person: p}
	mapping = m.TypeMap(reflect.TypeOf(s))
	for _, key := range []string{"id", "name", "wears_glasses", "weight", "age"} {
		fi := mapping.GetByPath(key)
		assert.NotNil(t, fi, "Expecting to find key %s in mapping", key)
	}

	type RugbyPlayer struct {
		Position   int
		IsIntense  bool `db:"is_intense"`
		IsAllBlack bool `db:"-"`
		SportsPerson
	}
	r := RugbyPlayer{12, true, false, s}
	mapping = m.TypeMap(reflect.TypeOf(r))
	for _, key := range []string{"id", "name", "wears_glasses", "weight", "age", "position", "is_intense"} {
		fi := mapping.GetByPath(key)
		assert.NotNil(t, fi, "Expecting to find key %s in mapping", key)
	}

	fi := mapping.GetByPath("isallblack")
	assert.Nil(t, fi, "Expecting to ignore `IsAllBlack` field")
}

func TestGetByTraversal(t *testing.T) {
	type C struct {
		C0 int
		C1 int
	}
	type B struct {
		B0 string
		B1 *C
	}
	type A struct {
		A0 int
		A1 B
	}

	testCases := []struct {
		Index        []int
		ExpectedName string
		ExpectNil    bool
	}{
		{
			Index:        []int{0},
			ExpectedName: "A0",
		},
		{
			Index:        []int{1, 0},
			ExpectedName: "B0",
		},
		{
			Index:        []int{1, 1, 1},
			ExpectedName: "C1",
		},
		{
			Index:     []int{3, 4, 5},
			ExpectNil: true,
		},
		{
			Index:     []int{},
			ExpectNil: true,
		},
		{
			Index:     nil,
			ExpectNil: true,
		},
	}

	m := NewMapperFunc("db", func(n string) string { return n })
	tm := m.TypeMap(reflect.TypeOf(A{}))

	for i, tc := range testCases {
		fi := tm.GetByTraversal(tc.Index)
		if tc.ExpectNil {
			assert.Nil(t, fi, "%d: expected nil", i)
			continue
		}

		require.NotNil(t, fi, "%d: expected %s, got nil", i, tc.ExpectedName)
		assert.Equal(t, tc.ExpectedName, fi.Name, "%d: name mismatch", i)
	}
}

// TestMapperMethodsByName tests Mapper methods FieldByName and TraversalsByName
func TestMapperMethodsByName(t *testing.T) {
	type C struct {
		C0 string
		C1 int
	}
	type B struct {
		B0 *C     `db:"B0"`
		B1 C      `db:"B1"`
		B2 string `db:"B2"`
	}
	type A struct {
		A0 *B `db:"A0"`
		B  `db:"A1"`
		A2 int
		a3 int //nolint
	}

	val := &A{
		A0: &B{
			B0: &C{C0: "0", C1: 1},
			B1: C{C0: "2", C1: 3},
			B2: "4",
		},
		B: B{
			B0: nil,
			B1: C{C0: "5", C1: 6},
			B2: "7",
		},
		A2: 8,
	}

	testCases := []struct {
		Name            string
		ExpectInvalid   bool
		ExpectedValue   interface{}
		ExpectedIndexes []int
	}{
		{
			Name:            "A0.B0.C0",
			ExpectedValue:   "0",
			ExpectedIndexes: []int{0, 0, 0},
		},
		{
			Name:            "A0.B0.C1",
			ExpectedValue:   1,
			ExpectedIndexes: []int{0, 0, 1},
		},
		{
			Name:            "A0.B1.C0",
			ExpectedValue:   "2",
			ExpectedIndexes: []int{0, 1, 0},
		},
		{
			Name:            "A0.B1.C1",
			ExpectedValue:   3,
			ExpectedIndexes: []int{0, 1, 1},
		},
		{
			Name:            "A0.B2",
			ExpectedValue:   "4",
			ExpectedIndexes: []int{0, 2},
		},
		{
			Name:            "A1.B0.C0",
			ExpectedValue:   "",
			ExpectedIndexes: []int{1, 0, 0},
		},
		{
			Name:            "A1.B0.C1",
			ExpectedValue:   0,
			ExpectedIndexes: []int{1, 0, 1},
		},
		{
			Name:            "A1.B1.C0",
			ExpectedValue:   "5",
			ExpectedIndexes: []int{1, 1, 0},
		},
		{
			Name:            "A1.B1.C1",
			ExpectedValue:   6,
			ExpectedIndexes: []int{1, 1, 1},
		},
		{
			Name:            "A1.B2",
			ExpectedValue:   "7",
			ExpectedIndexes: []int{1, 2},
		},
		{
			Name:            "A2",
			ExpectedValue:   8,
			ExpectedIndexes: []int{2},
		},
		{
			Name:            "XYZ",
			ExpectInvalid:   true,
			ExpectedIndexes: []int{},
		},
		{
			Name:            "a3",
			ExpectInvalid:   true,
			ExpectedIndexes: []int{},
		},
	}

	// build the names array from the test cases
	names := make([]string, len(testCases))
	for i, tc := range testCases {
		names[i] = tc.Name
	}
	m := NewMapperFunc("db", func(n string) string { return n })
	v := reflect.ValueOf(val)
	values := m.FieldsByName(v, names)
	require.Len(t, values, len(testCases))
	indexes := m.TraversalsByName(v.Type(), names)
	require.Len(t, indexes, len(testCases))
	for i, val := range values {
		tc := testCases[i]
		traversal := indexes[i]
		require.Equal(t, tc.ExpectedIndexes, traversal)
		val = reflect.Indirect(val)
		if tc.ExpectInvalid {
			assert.False(t, val.IsValid(), "%d: expected zero value, got %v", i, val)
			continue
		}
		require.True(t, val.IsValid(), "%d: expected valid value", i)
		actualValue := reflect.Indirect(val).Interface()
		assert.Equal(t, tc.ExpectedValue, actualValue, "%d: value mismatch", i)
	}
}

func TestFieldByIndexes(t *testing.T) {
	type C struct {
		C0 bool
		C1 string
		C2 int
		C3 map[string]int
	}
	type B struct {
		B1 C
		B2 *C
	}
	type A struct {
		A1 B
		A2 *B
	}
	testCases := []struct {
		value         interface{}
		indexes       []int
		expectedValue interface{}
		readOnly      bool
	}{
		{
			value: A{
				A1: B{B1: C{C0: true}},
			},
			indexes:       []int{0, 0, 0},
			expectedValue: true,
			readOnly:      true,
		},
		{
			value: A{
				A2: &B{B2: &C{C1: "answer"}},
			},
			indexes:       []int{1, 1, 1},
			expectedValue: "answer",
			readOnly:      true,
		},
		{
			value:         &A{},
			indexes:       []int{1, 1, 3},
			expectedValue: map[string]int{},
		},
	}

	for i, tc := range testCases {
		checkResults := func(v reflect.Value) {
			if tc.expectedValue == nil {
				assert.True(t, v.IsNil(), "%d: expected nil", i)
			} else {
				assert.Equal(t, tc.expectedValue, v.Interface(), "%d: value mismatch", i)
			}
		}

		checkResults(FieldByIndexes(reflect.ValueOf(tc.value), tc.indexes))
		if tc.readOnly {
			checkResults(FieldByIndexesReadOnly(reflect.ValueOf(tc.value), tc.indexes))
		}
	}
}

func TestMustBe(t *testing.T) {
	typ := reflect.TypeOf(E1{})
	mustBe(typ, reflect.Struct)

	defer func() {
		r := recover()
		require.NotNil(t, r, "expected panic")
		valueErr, ok := r.(*reflect.ValueError)
		require.True(t, ok, "expected panic with *reflect.ValueError, got %T", r)
		assert.Equal(t, reflect.String, valueErr.Kind)
	}()

	typ = reflect.TypeOf("string")
	mustBe(typ, reflect.Struct)
	t.Error("got here, didn't expect to")
}

type E1 struct {
	A int
}
type E2 struct {
	E1
	B int
}
type E3 struct {
	E2
	C int
}
type E4 struct {
	E3
	D int
}

func BenchmarkFieldNameL1(b *testing.B) {
	e4 := E4{D: 1}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.FieldByName("D")
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldNameL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.FieldByName("A")
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldPosL1(b *testing.B) {
	e4 := E4{D: 1}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.Field(1)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldPosL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := v.Field(0)
		f = f.Field(0)
		f = f.Field(0)
		f = f.Field(0)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkFieldByIndexL4(b *testing.B) {
	e4 := E4{}
	e4.A = 1
	idx := []int{0, 0, 0, 0}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(e4)
		f := FieldByIndexes(v, idx)
		if f.Interface().(int) != 1 {
			b.Fatal("Wrong value.")
		}
	}
}

func BenchmarkTraversalsByName(b *testing.B) {
	type A struct {
		Value int
	}

	type B struct {
		A A
	}

	type C struct {
		B B
	}

	type D struct {
		C C
	}

	m := NewMapper("")
	t := reflect.TypeOf(D{})
	names := []string{"C", "B", "A", "Value"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if l := len(m.TraversalsByName(t, names)); l != len(names) {
			b.Errorf("expected %d values, got %d", len(names), l)
		}
	}
}

func BenchmarkTraversalsByNameFunc(b *testing.B) {
	type A struct {
		Z int
	}

	type B struct {
		A A
	}

	type C struct {
		B B
	}

	type D struct {
		C C
	}

	m := NewMapper("")
	t := reflect.TypeOf(D{})
	names := []string{"C", "B", "A", "Z", "Y"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var l int

		if err := m.TraversalsByNameFunc(t, names, func(_ int, _ []int) error {
			l++
			return nil
		}); err != nil {
			b.Errorf("unexpected error %s", err)
		}

		if l != len(names) {
			b.Errorf("expected %d values, got %d", len(names), l)
		}
	}
}
