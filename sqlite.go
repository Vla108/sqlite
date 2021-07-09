package sqlite

import (
	"database/sql/driver"
	"fmt"

	"encoding/json"
	"reflect"
	"strings"

	//"strconv"
	//"time"

	db "modernc.org/sqlite"
)

type SQLITE struct {
	SQL  db.Driver
	Conn driver.Conn
}
type ROWS struct {
	rows driver.Rows
	vals []driver.Value
}

type ERESULT struct {
	RowAffected  int64
	LastInsertID int64
}

func (sql *SQLITE) Connect(fname string) error {
	fmt.Println("Connect SQLITE to", fname)
	var e error
	sql.Conn, e = sql.SQL.Open(fname)
	if e != nil {
		fmt.Println(e.Error())
		return e
	}
	return nil
}
func (sql *SQLITE) Close() {
	sql.Conn.Close()
}
func (r *ROWS) Close() {
	if r.rows == nil {
		return
	}
	r.rows.Close()
	r.rows = nil
}
func (r *ROWS) Next() bool {
	e := r.rows.Next(r.vals)
	if e != nil {
		return false
	}
	return true
}
func (r *ROWS) Scan(val ...interface{}) {
	vc := len(val)
	//dc := len(r.vals)
	//fmt.Println("Sacan ", vc, " values from ", dc, " dbvaluse")
	for i, v := range r.vals {
		if i >= vc {
			return
		}
		//fmt.Println(v)
		if val[i] == nil {
			continue
		}

		t := reflect.TypeOf(val[i]).String()
		if t[0] != '*' {
			fmt.Println("WARNING SQLITE: Scan attributes must be pointer: column ", i)
			continue
		} else {
			t = t[1:]
		}
		rt := reflect.TypeOf(v)
		if rt == nil {
			//fmt.Println("WARNING SQLITE: Unknown Type of dbvalue column ", i)
			//fmt.Println("v=", v)
			if t == "bool" {
				sa := val[i].(*bool)
				*sa = false
			}
			continue
		}
		vt := reflect.TypeOf(v).String()

		//fmt.Println(t, vt)
		isstruct := false
		if strings.Contains(t, ".") {
			t = "string"
			isstruct = true
		}

		if t != vt {
			if t == "bool" || t == "int" {

			} else {
				fmt.Println("WARNING SQLITE: Type of scan value not equal dbvalue", t, "!=", vt, " column", i)
				continue
			}

		}
		switch t {
		case "string":

			if isstruct {
				fmt.Println("struct", vt)
				json.Unmarshal([]byte(strings.ReplaceAll(v.(string), "''", "'")), val[i])
			} else {
				sa := val[i].(*string)
				*sa = strings.ReplaceAll(v.(string), "''", "'")
			}

		case "int64":
			sa := val[i].(*int64)
			*sa = v.(int64)
		case "float64":
			sa := val[i].(*float64)
			*sa = v.(float64)
		case "bool":
			sa := val[i].(*bool)
			*sa = (v.(int64) == 1)

		case "int":
			sa := val[i].(*int)
			*sa = int(v.(int64))

		}

	}
}

func (sql *SQLITE) Query(q string) ROWS {

	fmt.Println(q)
	var ret ROWS
	var e error
	if sql.Conn == nil {
		fmt.Println("ERROR SQLITE: Connection is nil")
		return ret
	}
	pq, pqe := sql.Conn.Prepare(q)
	if pqe != nil {
		fmt.Println(pqe.Error())
		return ret
	}
	ret.rows, e = pq.Query(nil)
	if e != nil {
		fmt.Println(e.Error())
		return ret
	}

	pq.Close()
	var dv driver.Value
	for range ret.rows.Columns() {
		ret.vals = append(ret.vals, dv)
	}
	return ret
}
func (sql *SQLITE) EXEC(q string) ERESULT {

	fmt.Println(q)
	var ret ERESULT

	var e error
	if sql.Conn == nil {
		fmt.Println("ERROR SQLITE: Connection is nil")
		return ret
	}
	pq, pqe := sql.Conn.Prepare(q)
	if pqe != nil {
		fmt.Println(pqe.Error())
		return ret
	}
	var res driver.Result
	res, e = pq.Exec(nil)
	if e != nil {
		fmt.Println(e.Error())
		return ret
	}
	ret.RowAffected, _ = res.RowsAffected()
	ret.LastInsertID, _ = res.LastInsertId()
	pq.Close()

	return ret
}

func (sql *SQLITE) Update(tbs string, where string, feilds string, val ...interface{}) ERESULT {
	vc := len(val) - 1
	ss := strings.Split(feilds, ",")

	q := "UPDATE " + tbs + " SET "
	fc := len(ss) - 1
	for i := range ss {
		v := ""
		t := reflect.TypeOf(val[i]).Kind()
		//fmt.Println("type=", t, reflect.TypeOf(val[i]))
		//fmt.Println(reflect.TypeOf(val[i]).Kind())
		if t == reflect.Struct {
			d, _ := json.MarshalIndent(val[i], "", "")
			v = "'" + strings.ReplaceAll(string(d), "'", `''`) + "'"

		} else {
			v = fmt.Sprint(val[i])
		}

		if t == reflect.String {
			v = "'" + strings.ReplaceAll(v, "'", `''`) + "'"
		}
		q += ss[i] + "=" + v

		if i >= vc {
			break
		}
		if i != fc {
			q += ","
		}
	}
	//q = strings.TrimRight(q, ",")
	q += " WHERE " + where + " ;"
	//fmt.Println(q)
	vc += 1
	if len(ss) != vc {
		fmt.Println("\nWARNING SQLITE: Update count feilds!=values  ->", len(ss), "!=", vc)
		//fmt.Println(q)
	}
	return sql.EXEC(q)
}
func (sql *SQLITE) Insert(tbs string, feilds string, val ...interface{}) ERESULT {
	vc := len(val) - 1
	ss := strings.Split(feilds, ",")

	q := "INSERT INTO  " + tbs + " "
	fc := len(ss) - 1
	ft := "("
	vt := " VALUES ("
	for i := range ss {
		v := ""
		t := reflect.TypeOf(val[i]).Kind()
		//fmt.Println("type=", t)
		//fmt.Println(reflect.TypeOf(val[i]).Kind())
		if t == reflect.Struct {
			d, _ := json.MarshalIndent(val[i], "", "")
			v = "'" + strings.ReplaceAll(string(d), "'", `''`) + "'"

		} else {
			v = fmt.Sprint(val[i])
		}
		if t == reflect.String {
			v = "\"" + strings.ReplaceAll(v, "\"", "'") + "\""
		}
		ft += ss[i]
		vt += v
		if i >= vc {
			break
		}
		if i != fc {
			ft += ","
			vt += ","
		}

	}
	q += ft + ")" + vt + ") ;"
	vc += 1
	if len(ss) != vc {
		fmt.Println("WARNING SQLITE: Insert count feilds!=values ->", len(ss), "!=", vc)
	}
	//fmt.Println(q)

	return sql.EXEC(q)
}

/*
func (sql *SQLITE) UpdOrInsert(tbs string, where string, feilds string, val ...interface{}) ERESULT {
	r := sql.Update(tbs, where, feilds, val)
	if r.RowAffected == 0 {
		r = sql.Insert(tbs, feilds, val)
	}
	return r
}
*/
