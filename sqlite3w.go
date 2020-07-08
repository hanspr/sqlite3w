package sqlite3w

import (
	//"errors"
	"os"
	"reflect"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type Sqlite3w struct {
	path        string
	create      bool
	stopOnError bool
	conn        *sqlite3.Conn
	stmt        *sqlite3.Stmt
	err         error
}

func New() *Sqlite3w {
	rs := new(Sqlite3w)
	rs.stopOnError = true
	rs.create = true
	return rs
}

func (rs *Sqlite3w) Connect(path string) error {
	if !rs.create {
		_, err := os.Stat(path)
		if err != nil {
			if !rs.create {
				if rs.stopOnError {
					panic(rs.err)
				}
				return err
			}
		}
	}
	rs.path = path
	rs.conn, rs.err = sqlite3.Open(path)
	if rs.err != nil {
		if rs.stopOnError {
			panic(rs.err)
		}
		return rs.err
	}
	defer rs.conn.Close()
	return nil
}

func (rs *Sqlite3w) PrepareExecute(qry string) {
	stmt, err := rs.conn.Prepare(qry)
	if err != nil {
		if rs.stopOnError {
			panic(rs.err)
		}
		return
	}
	rs.stmt = stmt
	defer stmt.Close()
}

func (rs *Sqlite3w) Do(qry string) {
	err := rs.conn.Exec(qry)
	if err != nil {
		if rs.stopOnError {
			panic(rs.err)
		}
		return
	}
}

//struct {
//	cid  int    `column:name`
//	cstr string `column:name`
//  ...
//}

func (rs *Sqlite3w) FetchStruct(s interface{}) bool {
	colidx := make(map[string]int)

	// Clear all values of the struct
	p := reflect.ValueOf(s).Elem()
	p.Set(reflect.Zero(p.Type()))

	hasRow, err := rs.stmt.Step()
	if err != nil {
		if rs.stopOnError {
			panic(rs.err)
		}
		return false
	}
	if !hasRow {
		return false
	}
	for i := 0; i < rs.stmt.ColumnCount(); i++ {
		colidx[rs.stmt.ColumnName(i)] = i
	}
	t := reflect.TypeOf(s)
	tv := reflect.ValueOf(s)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		index, ok := colidx[f.Tag.Get("column")]
		if ok {
			switch t.Kind() {
			case reflect.Int:
				val, ok, err := rs.stmt.ColumnInt64(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetInt(val)
				}
			case reflect.String:
				val, ok, err := rs.stmt.ColumnText(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetString(val)
				}
			case reflect.Float64:
				val, ok, err := rs.stmt.ColumnDouble(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetFloat(val)
				}
			}
		}
	}
	return true
}
