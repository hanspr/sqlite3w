package sqlite3w

import (
	"os"
	"reflect"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type Sqlite3w struct {
	path        string
	Create      bool
	StopOnError bool
	conn        *sqlite3.Conn
	Stmt        *sqlite3.Stmt
	err         error
	colidx      map[string]int
	EOF         bool
}

func New() *Sqlite3w {
	rs := new(Sqlite3w)
	rs.StopOnError = true
	rs.Create = false
	rs.colidx = make(map[string]int)
	rs.EOF = false
	return rs
}

func (rs *Sqlite3w) Connect(path string) error {
	if !rs.create {
		_, err := os.Stat(path)
		if err != nil {
			if !rs.Create {
				if rs.StopOnError {
					panic("path does not exists " + path)
				}
				return err
			}
		}
	}
	rs.path = path
	rs.conn, rs.err = sqlite3.Open(path)
	if rs.err != nil {
		if rs.StopOnError {
			panic(rs.err)
		}
		return rs.err
	}
	return nil
}

func (rs *Sqlite3w) Execute(qry string, args ...interface{}) {
	rs.EOF = false
	Stmt, err := rs.conn.Prepare(qry, args...)
	if err != nil {
		if rs.StopOnError {
			panic(err)
		}
		return
	}
	rs.Stmt = stmt
	// Get first row
	hasRow, err := rs.Stmt.Step()
	if err != nil {
		if rs.StopOnError {
			panic(rs.err)
		}
		rs.EOF = true
		return
	}
	if !hasRow {
		rs.EOF = true
		return
	}
	// Delete information on every new query
	for k := range rs.colidx {
		delete(rs.colidx, k)
	}
	// Get column indexes
	for i := 0; i < rs.Stmt.ColumnCount(); i++ {
		rs.colidx[rs.Stmt.ColumnName(i)] = i
	}
}

func (rs *Sqlite3w) Do(qry string, args ...interface{}) {
	err := rs.conn.Exec(qry, args...)
	if err != nil {
		if rs.StopOnError {
			panic(err)
		}
		return
	}
}

//struct {
//	cid  int    `column:name`
//	cstr string `column:name`
//  ...
//}
// FecthStruct(s strcut)
func (rs *Sqlite3w) FetchStruct(s interface{}) bool {

	// Clear all values of the struct
	p := reflect.ValueOf(s).Elem()
	p.Set(reflect.Zero(p.Type()))

	if rs.EOF {
		return false
	}

	tv := reflect.ValueOf(s)
	t := tv.Type().Elem()

	for i := 0; i < tv.Elem().NumField(); i++ {
		f := t.Field(i)
		index, ok := rs.colidx[f.Tag.Get("column")]
		if ok {
			switch f.Type.Kind() {
			case reflect.Int:
				val, ok, err := rs.Stmt.ColumnInt64(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetInt(val)
				}
			case reflect.String:
				val, ok, err := rs.Stmt.ColumnText(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetString(val)
				}
			case reflect.Float64:
				val, ok, err := rs.Stmt.ColumnDouble(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetFloat(val)
				}
			}
		}
	}
	hasRow, err := rs.Stmt.Step()
	if err != nil {
		if rs.StopOnError {
			panic(rs.err)
		}
		rs.EOF = true
		return false
	}
	if !hasRow {
		rs.EOF = true
	}
	return true
}
