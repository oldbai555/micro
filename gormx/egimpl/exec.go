package egimpl

import (
	"database/sql"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/oldbai555/lbtool/extpkg/pie/pie"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/micro/uctx"
	"gorm.io/gorm"
	"strconv"
)

func toRowsRangeStr(rows int) string {
	if rows < 100 {
		return "0-100"
	} else if rows < 500 {
		return "100-500"
	} else if rows < 1000 {
		return "500-1000"
	} else if rows < 2000 {
		return "1000-2000"
	}
	return ">=2000"
}

type sqlExecType int

const (
	sqlExecTypeInsert      sqlExecType = 1
	sqlExecTypeUpdate      sqlExecType = 2
	sqlExecTypeDelete      sqlExecType = 3
	sqlExecTypeSelect      sqlExecType = 4
	sqlExecTypeDDL         sqlExecType = 5
	sqlExecTypeCreateTable sqlExecType = 6
)

func (p sqlExecType) String() string {
	switch p {
	case sqlExecTypeInsert:
		return "INSERT"
	case sqlExecTypeUpdate:
		return "UPDATE"
	case sqlExecTypeDelete:
		return "DELETE"
	case sqlExecTypeSelect:
		return "SELECT"
	case sqlExecTypeDDL:
		return "DDL"
	}
	return "OTHER"
}

type SqlParsedResult struct {
	typ           sqlExecType
	ddlAction     string
	tableList     []string
	sqlRemovedVal string
	valTupleCount int
	corpId        uint32
}

func ParseSql(s string) (*SqlParsedResult, error) {
	var r SqlParsedResult
	walkNode := func(node sqlparser.SQLNode) {
		switch v := node.(type) {
		case *sqlparser.SQLVal:
			v.Val = []byte("?")
		case *sqlparser.ComparisonExpr:
			if v.Left != nil && v.Right != nil {
				if c, ok := v.Left.(*sqlparser.ColName); ok {
					if c.Name.String() == "corp_id" {
						if ri, ok := v.Right.(*sqlparser.SQLVal); ok {
							if ri.Type == sqlparser.IntVal || ri.Type == sqlparser.StrVal {
								corpId, err := strconv.ParseUint(string(ri.Val), 10, 32)
								if err == nil {
									r.corpId = uint32(corpId)
								}
							}
						}
					}
				}
			}
		case sqlparser.SelectExprs:
			r.typ = sqlExecTypeSelect
		case *sqlparser.Select:
			r.typ = sqlExecTypeSelect
		case *sqlparser.Update:
			r.typ = sqlExecTypeUpdate
		case *sqlparser.UpdateExpr:
			r.typ = sqlExecTypeUpdate
		case *sqlparser.Delete:
			r.typ = sqlExecTypeDelete
		case *sqlparser.Insert:
			r.typ = sqlExecTypeInsert
		case sqlparser.ValTuple:
			r.valTupleCount++
		case sqlparser.TableName:
			n := v.Name.String()
			if n != "" {
				if !pie.Strings(r.tableList).Contains(n) {
					r.tableList = append(r.tableList, n)
				}
			}
		case *sqlparser.DDL:
			r.typ = sqlExecTypeDDL
			r.ddlAction = v.Action
		case *sqlparser.CreateTable:
			r.typ = sqlExecTypeCreateTable
		}
	}

	stmt, err := sqlparser.Parse(s)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	walkNode(stmt)
	err = stmt.WalkSubtree(func(node sqlparser.SQLNode) (bool, error) {
		walkNode(node)
		return true, nil
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	bu := sqlparser.NewTrackedBuffer(nil)
	stmt.Format(bu)
	r.sqlRemovedVal = bu.String()

	return &r, nil
}

type TraceInfo struct {
	Timestamp uint32 `json:"timestamp"`
	Sql       string `json:"sql"`
	Rows      uint32 `json:"rows"`
	Duration  uint32 `json:"duration"`
	ReqId     string `json:"req_id"`
	BizCode   int    `json:"biz_code"`
	ErrMsg    string `json:"err_msg"`
	Type      string `json:"type"`
	Tables    string `json:"tables"`
	CorpId    uint32 `json:"corp_id"`
	Module    string `json:"module"`
	Context   string `json:"context"`
	DbId      string `json:"db_id"`
	ClientIp  string `json:"client_ip"`
}

func dbExec(ctx uctx.IUCtx, db *gorm.DB, sql string, values ...interface{}) *gorm.DB {
	res := db.Exec(sql, values...)
	if res.Error != nil {
		log.Errorf("err:%v", res.Error)
	}
	return res
}

type Rows struct {
	cols   []string
	colMap map[string]int
	rows   [][]string
}

const (
	MaxQueryPacketBytes = int64(200 * 1024 * 1024)
)

func RawQuery(ctx uctx.IUCtx, db *gorm.DB, sqlQuery string, sqlValues ...interface{}) (res *Rows, err error) {
	var bytesRead = int64(0)
	rows, err := db.Raw(sqlQuery, sqlValues...).Rows()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	res = &Rows{
		cols:   cols,
		colMap: map[string]int{},
	}
	for i, v := range cols {
		res.colMap[v] = i
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		var row []string
		for _, v := range values {
			var val string
			if v != nil {
				val = string(v)
				bytesRead += int64(len(val))
			}
			row = append(row, val)
		}

		if bytesRead > MaxQueryPacketBytes {
			log.Errorf("exceeded max allowed packet size %d", MaxQueryPacketBytes)
			return nil, lberr.NewErr(-5, "exceeded max allowed packet size")
		}

		res.rows = append(res.rows, row)
	}
	return res, nil
}

type Option struct {
	codeFileLineFunc string
	ignoreBroken     bool
	objType          string
}
