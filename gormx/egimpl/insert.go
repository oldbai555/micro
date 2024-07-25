/**
 * @Author: zjj
 * @Date: 2024/7/25
 * @Desc:
**/

package egimpl

import (
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/micro/gormx/engine"
	"github.com/oldbai555/micro/uctx"
	"gorm.io/gorm"
	"reflect"
	"time"
)

func (g *GormEngine) InsertModel(ctx uctx.IUCtx, req *engine.InsertModelReq) (*engine.InsertModelRsp, error) {
	var rsp engine.InsertModelRsp

	objType, ok := g.objTypeMgr[req.ObjType]
	if !ok {
		panic(fmt.Sprintf("not found obj type %s", req.ObjType))
	}

	var j map[string]interface{}
	err := decodeJson(req.JsonData, &j)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	modMap := map[string]interface{}{}
	now := uint32(time.Now().Unix())
	for _, v := range objType.FieldList.List {
		if v.FieldName == createdAt && v.Type == "uint32" {
			setIfZero(j, createdAt, now)
		} else if v.FieldName == updatedAt && v.Type == "uint32" {
			setIfZero(j, updatedAt, now)
		} else if v.FieldName == deletedAt && v.Type == "uint32" {
			setIfZero(j, deletedAt, 0)
		}
	}

	func(keys ...string) {
		for _, key := range keys {
			if v, ok := j[key]; ok {
				modMap[key] = v
				delete(j, key)
			}
		}
	}(req.Skips...)

	modMap, err = adjustJson(objType, j, modMap)
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, err
	}

	err = compareDbColAndAdjust(objType, j)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 返回主键id
	const (
		idKey = "id"
	)

	// 没想好怎么拿
	var pkField *engine.ObjectField
	for _, v := range objType.FieldList.List {
		if v.FieldName == idKey {
			pkField = v
			break
		}
	}

	if pkField != nil {
		_, ok = j[idKey]
		if !ok {
			j[idKey] = 0
		}
	}

	// 执行原生SQL貌似拿插入的ID有点非常的麻烦
	res := g.db.Table(quoteName(req.Table)).Create(j)
	err = res.Error
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var lastId interface{}
	if pkField != nil {
		insertIDKey := fmt.Sprintf("@%s", idKey)
		lastId, ok := j[insertIDKey]
		if ok {
			j[idKey] = lastId
			delete(j, insertIDKey)
		}
	}

	if pkField != nil {
		idGiven := j[idKey]
		isZero := false
		if idGiven == nil {
			isZero = true
		} else {
			switch x := idGiven.(type) {
			case json.Number:
				isZero = x == "0"
			case jsoniter.Number:
				isZero = x == "0"
			case string:
				isZero = x == "0"
			default:
				isZero = reflect.Zero(reflect.TypeOf(idGiven)) == idGiven
			}
		}
		if isZero {
			if pkField.Type == "uint32" {
				j[idKey] = lastId
			} else if pkField.Type == "uint64" {
				j[idKey] = lastId
			} else {
				log.Warnf("unsupported type %s", pkField.Type)
			}
		} else {
			//log.Warnf("id not zero %v", idGiven)
		}
	} else {
		log.Warnf("not found primary key")
	}

	// 较正之前的字段返回一下
	for k, v := range modMap {
		j[k] = v
	}

	buf, err := jsoniter.Marshal(j)
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, err
	}
	rsp.JsonData = string(buf)
	return &rsp, nil
}

func (g *GormEngine) BatchInsertModel(ctx uctx.IUCtx, req *engine.BatchInsertModelReq) (*engine.BatchInsertModelRsp, error) {
	var rsp engine.BatchInsertModelRsp

	return &rsp, nil
}

func setIfZero(j map[string]interface{}, k string, v uint32) {
	o := j[k]
	if o == nil {
		j[k] = v
	} else {
		switch r := o.(type) {
		case jsoniter.Number:
			if r == "0" {
				j[k] = v
			}
		case json.Number:
			if r == "0" {
				j[k] = v
			}
		case float64, int:
			if r == 0 {
				j[k] = v
			}
		}
	}
}

func adjustJson(objType *engine.ModelObjectType, j map[string]interface{}, modMap map[string]interface{}) (map[string]interface{}, error) {
	if modMap == nil {
		modMap = map[string]interface{}{}
	}

	fieldMap := map[string]*engine.ObjectField{}
	for _, v := range objType.FieldList.List {
		fieldMap[v.FieldName] = v
	}

	var delList []string
	// 支持 map + slice
	for k, v := range j {
		objField := fieldMap[k]
		if objField == nil {
			delList = append(delList, k)
			continue
		}

		if v == nil {
			modMap[k] = v
			j[k] = ""
			continue
		}

		switch x := v.(type) {
		case map[string]interface{}:
			if !isObjectField(objType, k) {
				expr := x["expr"]
				if expr != nil {
					if exprStr, ok := expr.(string); ok && exprStr != "" {
						args := x["args"]
						var argsList []interface{}
						if args != nil {
							argsList, _ = args.([]interface{})
						}
						if len(argsList) == 0 {
							j[k] = gorm.Expr(exprStr)
						} else {
							j[k] = gorm.Expr(exprStr, argsList...)
						}
					}
				}
			} else {
				logic := func() error {
					buf, err := json.Marshal(x)
					if err != nil {
						log.Errorf("err:%s", err)
						return err
					}
					j[k] = string(buf)
					modMap[k] = v
					return nil
				}
				expr := x["expr"]
				if expr != nil {
					if exprStr, ok := expr.(string); ok && exprStr != "" {
						args := x["args"]
						var argsList []interface{}
						if args != nil {
							argsList, _ = args.([]interface{})
						}
						if len(argsList) == 0 {
							j[k] = gorm.Expr(exprStr)
						} else {
							j[k] = gorm.Expr(exprStr, argsList...)
						}
					} else {
						if err := logic(); err != nil {
							log.Errorf("err:%s", err)
							return modMap, err
						}
					}
				} else {
					if err := logic(); err != nil {
						log.Errorf("err:%s", err)
						return modMap, err
					}
				}
			}

		case []interface{}:
			buf, err := json.Marshal(x)
			if err != nil {
				log.Errorf("err:%s", err)
				return modMap, err
			}
			j[k] = string(buf)
			modMap[k] = v

		case bool:
			modMap[k] = x
			if x {
				j[k] = 1
			} else {
				j[k] = 0
			}
		}

	}
	for _, v := range delList {
		delete(j, v)
	}
	return modMap, nil
}

func isObjectField(objType *engine.ModelObjectType, fieldName string) bool {
	for _, v := range objType.FieldList.List {
		if v.FieldName == fieldName && v.Type == "object" {
			return true
		}
	}
	return false
}

// 跳过数据库没有定义的列
func compareDbColAndAdjust(objType *engine.ModelObjectType, j map[string]interface{}) error {
	return nil
}

func ProcessingInsertValuesWithCrypt(j map[string]interface{}) (keys []string, values []interface{}, qs []string, err error) {
	for k, v := range j {
		keys = append(keys, quoteName(k))
		values = append(values, v)
		qs = append(qs, "?")
	}
	return
}

func IsMysqlUniqueIndexConflictError(err error) (isDup bool) {
	isDup = false
	if err == nil {
		return
	}
	if x, ok := err.(*mysql.MySQLError); ok {
		if x.Number == 1062 {
			isDup = true
		}
	}
	return
}