package sqlite3w

import (
	"fmt"
	"os"
	"reflect"
	"regexp"

	//"strconv"
	"strings"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
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

func (rs *Sqlite3w) InsertStruct(table string, s interface{}) {
	data := StructToMap(s)

	rs.InsertMap(table, data)
}

func (rs *Sqlite3w) InsertMap(table string, d map[string]string) {
	var columns, values string

	for col, val := range d {
		columns = columns + "'" + col + "',"
		val = strings.ReplaceAll(val, "'", "''")
		values = values + "'" + val + "',"
	}
	columns = strings.TrimSuffix(columns, ",")
	values = strings.TrimSuffix(values, ",")
	qry := "insert into " + table + " (" + columns + ") values (" + values + ")"
	rs.Do(qry)
}

func (rs *Sqlite3w) UpdateStruct(table, where string, s interface{}) {
	data := StructToMap(s)
	rs.UpdateMap(table, where, data)
}

func (rs *Sqlite3w) UpdateMap(table, where string, d map[string]string) {
	var values, qry string

	for col, val := range d {
		val = strings.ReplaceAll(val, "'", "''")
		values = values + col + "='" + val + "',"
	}
	values = strings.TrimSuffix(values, ",")
	if where != "" {
		qry = "update " + table + " set " + values + " where " + where
	} else {
		qry = "update " + table + " set " + values
	}
	rs.Do(qry)
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
			case reflect.Float64:
				val, ok, err := rs.Stmt.ColumnDouble(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetFloat(val)
				}
			default:
				val, ok, err := rs.Stmt.ColumnText(index)
				if err == nil && ok {
					tv.Elem().Field(i).SetString(val)
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

//func (rs *Sqlite3w) FetchMap as strings
func (rs *Sqlite3w) FetchMap() map[string]string {
	data := make(map[string]string)

	if rs.EOF {
		return nil
	}

	for col, i := range rs.colidx {
		v, ok, err := rs.Stmt.ColumnText(i)
		if ok && err == nil {
			data[col] = v
		} else {
			data[col] = ""
		}
	}
	hasRow, err := rs.Stmt.Step()
	if err != nil {
		if rs.StopOnError {
			panic(rs.err)
		}
		rs.EOF = true
		return nil
	}
	if !hasRow {
		rs.EOF = true
		return nil
	}
	return data
}

// General Functions

func StructToMap(s interface{}) map[string]string {
	data := make(map[string]string)
	tv := reflect.ValueOf(s)
	t := tv.Type().Elem()

	for i := 0; i < tv.Elem().NumField(); i++ {
		f := t.Field(i)
		if f.Tag.Get("column") != "" {
			switch f.Type.Kind() {
			case reflect.Int:
				data[f.Tag.Get("column")] = fmt.Sprintf("%v", tv.Elem().Field(i).Int())
			case reflect.Float64:
				data[f.Tag.Get("column")] = fmt.Sprintf("%v", tv.Elem().Field(i).Float())
			default:
				data[f.Tag.Get("column")] = tv.Elem().Field(i).String()
			}
		}
	}
	for k := range data {
		if data[k] == "" {
			delete(data, k)
		}
	}
	return data
}
