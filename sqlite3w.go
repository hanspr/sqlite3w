package sqlite3w

import (
	"os"
	"reflect"
	"regexp"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/ompluscator/dynamic-struct"
)

type Sqlite3w struct {
	path        string
	Create      bool
	StopOnError bool
	Conn        *sqlite3.Conn
	Stmt        *sqlite3.Stmt
	err         error
	colidx      map[string]int
	LastID      int64
	Changes     int
	EOF         bool
	reInsert    *regexp.Regexp
	data        *dynamicstruct.Builder
}

func New() *Sqlite3w {
	rs := new(Sqlite3w)
	rs.StopOnError = true
	rs.Create = false
	rs.colidx = make(map[string]int)
	rs.EOF = false
	rs.LastID = -1
	rs.reInsert = regexp.MustCompile(`(?i)^(?:\s*)insert`)
	return rs
}

func (rs *Sqlite3w) Connect(path string) error {
	if !rs.Create {
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
	rs.Conn, rs.err = sqlite3.Open(path)
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
	stmt, err := rs.Conn.Prepare(qry, args...)
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

//Do Exec : insert, delete, updates
func (rs *Sqlite3w) Do(qry string, args ...interface{}) {
	rs.LastID = -1
	rs.Changes = 0
	err := rs.Conn.Exec(qry, args...)
	if err != nil {
		if rs.StopOnError {
			panic(err)
		}
		return
	}
	if rs.reInsert.MatchString(qry) {
		rs.LastID = rs.Conn.LastInsertRowID()
	} else {
		rs.Changes = rs.Conn.Changes()
	}
}

func (rs *Sqlite3w) Insert(table string, s interface{}) {

}

func (rs *Sqlite3w) Update(table, where string, s interface{}) {

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

//func (rs *Sqlite3w) FetchMap
