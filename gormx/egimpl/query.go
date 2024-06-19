/**
 * @Author: zjj
 * @Date: 2024/6/18
 * @Desc:
**/

package egimpl

import (
	"encoding/json"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/micro/gormx/engine"
	"strings"
)

const (
	ErrCanNotSkipAllFields = 12001
)

const (
	createdAt = "created_at"
	updatedAt = "updated_at"
	deletedAt = "deleted_at"
)

func findFieldsByGetModelListReq(req *engine.GetModelListReq, objectType *engine.ModelObjectType) (string, error) {
	// var fields string
	if len(req.Fields) > 0 {
		addId := true

		q := make([]string, 0, len(req.Fields)+1)
		for _, v := range req.Fields {
			fieldName := quoteName(v)
			if fieldName == "id" {
				addId = false
			}
			q = append(q, fieldName)
		}
		if addId {
			q = append(q, "id")
		}
		return strings.Join(q, ","), nil
	}
	if len(req.Skips) > 0 {
		skipMap := map[string]bool{}
		for _, v := range req.Skips {
			skipMap[v] = true
		}
		var list []string
		for _, v := range objectType.FieldList.List {
			if !skipMap[v.FieldName] {
				list = append(list, v.FieldName)
			}
		}
		if len(list) == 0 {
			return "", lberr.NewErr(ErrCanNotSkipAllFields, "not skip all fields")
		}
		var q []string
		AddId := true
		for _, v := range list {
			fieldName := quoteName(v)
			if fieldName == "id" {
				AddId = false
			}
			q = append(q, fieldName)
		}
		if AddId {
			q = append(q, "id")
		}
		return strings.Join(q, ","), nil
	}
	return "*", nil
}

func quoteName(name string) string {
	if name != "" {
		if name[0] != '`' {
			q := true
			l := len(name)
			for i := 0; i < l; i++ {
				c := name[i]
				if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
					q = false
					break
				}
			}
			if q {
				name = fmt.Sprintf("`%s`", name)
			}
		}
	}
	return name
}

func hasDeletedAtField(objType *engine.ModelObjectType) bool {
	for _, v := range objType.FieldList.List {
		if v.FieldName == deletedAt && v.Type == "uint32" {
			return true
		}
	}
	return false
}

func rawResToListMap(objType *engine.ModelObjectType, res *Rows, returnUnknownFields bool) []map[string]interface{} {
	fieldTypeMap := make(map[string]*engine.ObjectField, len(objType.FieldList.List))
	for _, v := range objType.FieldList.List {
		fieldTypeMap[v.FieldName] = v
	}

	colFieldType := make([]*engine.ObjectField, len(res.cols))
	for i, v := range res.cols {
		colFieldType[i] = fieldTypeMap[v]
	}

	list := make([]map[string]any, len(res.rows))
	for i, row := range res.rows {
		rowMap := make(map[string]any, len(res.cols))
		for i, col := range res.cols {
			var fieldType *engine.ObjectField
			if colFieldType != nil && i < len(colFieldType) {
				fieldType = colFieldType[i]
			}
			if fieldType == nil {
				if returnUnknownFields {
					rowMap[col] = row[i]
				}
			} else {
				if row[i] == "" {
					continue
				}
				rowMap[col] = convertByFieldType(row[i], fieldType)
			}
		}

		list[i] = rowMap
	}

	return list
}
func convertByFieldType(s string, f *engine.ObjectField) interface{} {
	// 支持数组
	if f.IsArray {
		var list []interface{}
		if s != "" {
			err := decodeJson(s, &list)
			if err != nil {
				log.Errorf("err:%v", err)
			}
		}
		return list
	}

	switch f.Type {
	case "uint32", "uint64", "int32", "int64", "double", "float":
		return json.Number(s)
	case "bool":
		if s == "0" || s == "" {
			return false
		} else {
			return true
		}
	case "string", "bytes":
		return s
	case "object":
		// string to json
		var m map[string]interface{}
		if s != "" {
			err := decodeJson(s, &m)
			if err != nil {
				log.Errorf("err:%v", err)
			}
		}
		return m
	default:
		log.Errorf("Unsupported type %s", f.Type)
	}
	return s
}
