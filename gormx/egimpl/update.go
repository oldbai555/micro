/**
 * @Author: zjj
 * @Date: 2024/7/25
 * @Desc:
**/

package egimpl

import (
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/json"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/micro/gormx/engine"
	"github.com/oldbai555/micro/uctx"
	"strings"
	"time"
)

func (g *GormEngine) UpdateModel(ctx uctx.IUCtx, req *engine.UpdateModelReq) (*engine.UpdateModelRsp, error) {
	var rsp engine.UpdateModelRsp

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
		if v.FieldName == updatedAt && v.Type == "uint32" {
			setIfZero(j, updatedAt, now)
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
	if len(j) == 0 {
		return nil, lberr.NewInvalidArg("not found other params")
	}

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

	if hasDeletedAtField(objType) && !req.Unscoped {
		if req.Cond != "" {
			req.Cond += " AND "
		}
		req.Cond += "(deleted_at=0 OR deleted_at IS NULL)"
	}

	var cond string
	if req.Cond != "" {
		cond = fmt.Sprintf(" WHERE %s", req.Cond)
	}

	var limit string
	if req.Limit > 0 {
		limit = fmt.Sprintf(" LIMIT %d", req.Limit)
	}

	var fields []string
	var values []interface{}
	for k, v := range j {
		fields = append(fields, fmt.Sprintf("%s=?", quoteName(k)))
		values = append(values, v)
	}

	res := dbExec(
		ctx, g.db,
		fmt.Sprintf("UPDATE %s SET %s%s%s",
			quoteName(req.Table),
			strings.Join(fields, ","), cond, limit),
		values...,
	)
	err = res.Error
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.RowsAffected = uint64(res.RowsAffected)
	for k, v := range modMap {
		j[k] = v
	}

	buf, err := json.Marshal(j)
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, err
	}
	rsp.JsonData = string(buf)

	return &rsp, nil
}

func (g *GormEngine) SetModel(ctx uctx.IUCtx, req *engine.SetModelReq) (*engine.SetModelRsp, error) {
	var rsp engine.SetModelRsp

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
		if v.FieldName == updatedAt && v.Type == "uint32" {
			setIfZero(j, updatedAt, now)
		}
	}

	id := j["id"]
	if id == nil {
		return nil, lberr.NewInvalidArg("not found id")
	}

	req.Skips = append(req.Skips, "id", "created_at")
	func(keys ...string) {
		for _, key := range keys {
			if v, ok := j[key]; ok {
				modMap[key] = v
				delete(j, key)
			}
		}
	}(req.Skips...)
	if len(j) == 0 {
		return nil, lberr.NewInvalidArg("not found other params")
	}

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

	sqlStr := "UPDATE %s SET %s WHERE id=? %s"
	if hasDeletedAtField(objType) && !req.Unscoped {
		sqlStr += " AND (deleted_at=0 OR deleted_at IS NULL)"
	}

	var cond string
	if req.Cond != "" {
		cond = fmt.Sprintf(" AND %s", req.Cond)
	}

	var fields []string
	var values []interface{}
	for k, v := range j {
		fields = append(fields, fmt.Sprintf("%s=?", quoteName(k)))
		values = append(values, v)
	}
	j["id"] = convert2Uint64(id)
	values = append(values, j["id"])

	res := dbExec(
		ctx, g.db,
		fmt.Sprintf(sqlStr, quoteName(req.Table), strings.Join(fields, ","), cond),
		values...,
	)
	err = res.Error
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.RowsAffected = uint64(res.RowsAffected)
	for k, v := range modMap {
		j[k] = v
	}

	buf, err := sonic.Marshal(j)
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, err
	}
	rsp.JsonData = string(buf)

	return &rsp, nil
}
