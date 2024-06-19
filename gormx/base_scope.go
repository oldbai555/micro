package gormx

import (
	"errors"
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/lbtool/utils"
	"github.com/oldbai555/micro/core"
	"github.com/oldbai555/micro/gormx/engine"
	"github.com/oldbai555/micro/uctx"
	"reflect"
	"strings"

	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/proto"
)

type BaseScope[M any] struct {
	cond  Cond
	db    string
	table string
	m     *BaseModel[M]

	limit     uint32
	offset    uint32
	needCount bool

	selects []string
	skips   []string
	groups  []string
	orders  []string

	unscoped            bool
	returnUnknownFields bool

	corpId uint32

	caller string // 调用db的文件:行号.函数名

	ignoreConflict bool
	ignoreBroken   bool
}

func (p *BaseScope[M]) GetModel() *BaseModel[M] {
	return p.m
}

func (p *BaseScope[M]) SetTablePrefix(prefix string) *BaseScope[M] {
	p.cond.tablePrefix = prefix
	return p
}

// NeedCount 如果没有填，表示按照语义需要 count，如果填了的，就按照用户填写的配置
func (p *BaseScope[M]) NeedCount(b ...bool) *BaseScope[M] {
	if len(b) == 0 {
		p.needCount = true
		return p
	}

	p.needCount = b[0]
	return p
}

func (p *BaseScope[M]) IgnoreBroken(b ...bool) *BaseScope[M] {
	if len(b) == 0 {
		p.ignoreBroken = true
		return p
	}
	p.ignoreBroken = b[0]
	return p
}

func (p *BaseScope[M]) IgnoreConflict(b ...bool) *BaseScope[M] {
	if len(b) == 0 {
		p.ignoreConflict = true
		return p
	}
	p.ignoreConflict = b[0]
	return p
}

func (p *BaseScope[M]) WithDb(db string) *BaseScope[M] {
	p.db = db
	return p
}

func (p *BaseScope[M]) WithTable(table string) *BaseScope[M] {
	p.table = table
	return p
}

func (p *BaseScope[M]) Limit(limit uint32) *BaseScope[M] {
	p.limit = limit
	return p
}

func (p *BaseScope[M]) Offset(offset uint32) *BaseScope[M] {
	p.offset = offset
	return p
}

func (p *BaseScope[M]) Omit(columns ...string) *BaseScope[M] {
	p.skips = append(p.skips, columns...)
	return p
}

func (p *BaseScope[M]) CleanOmit() *BaseScope[M] {
	p.skips = make([]string, 0)
	return p
}

func (p *BaseScope[M]) Group(fields ...string) *BaseScope[M] {
	p.groups = append(p.groups, fields...)
	return p
}

func (p *BaseScope[M]) CleanGroup() *BaseScope[M] {
	p.groups = make([]string, 0)
	return p
}

func (p *BaseScope[M]) Select(fields ...string) *BaseScope[M] {
	p.selects = append(p.selects, fields...)
	return p
}

func (p *BaseScope[M]) CleanSelect() *BaseScope[M] {
	p.selects = make([]string, 0)
	return p
}

func (p *BaseScope[M]) NewList(listOption *core.ListOption) *BaseScope[M] {
	if listOption.Limit == 0 {
		p.Limit(defaultLimit)
	} else {
		p.Limit(listOption.Limit)
	}
	p.Offset(listOption.Offset)
	p.NeedCount(!listOption.SkipTotal)
	return p
}

func (p *BaseScope[M]) OrderAsc(fields ...string) *BaseScope[M] {
	for _, field := range fields {
		p.orders = append(p.orders, field+" ASC")
	}
	return p
}

func (p *BaseScope[M]) OrderDesc(fields ...string) *BaseScope[M] {
	for _, field := range fields {
		p.orders = append(p.orders, field+" DESC")
	}
	return p
}

func (p *BaseScope[M]) CleanOrder() *BaseScope[M] {
	p.orders = make([]string, 0)
	return p
}

func (p *BaseScope[M]) getOrder() string {
	return strings.Join(p.orders, ",")
}

func (p *BaseScope[M]) getGroup() string {
	return strings.Join(p.groups, ",")
}

func (p *BaseScope[M]) GetTableName() string {
	if p.table != "" {
		return p.table
	}
	return p.m.table
}

func (p *BaseScope[M]) newGetModelListReq() *engine.GetModelListReq {
	m := p.m
	return &engine.GetModelListReq{
		ObjType:             m.modelType,
		Table:               p.GetTableName(),
		Cond:                p.cond.ToString(),
		Offset:              p.offset,
		Limit:               p.limit,
		Fields:              p.selects,
		Order:               p.getOrder(),
		Group:               p.getGroup(),
		SkipCount:           !p.needCount,
		Skips:               p.skips,
		Unscoped:            p.unscoped,
		Db:                  p.db,
		ReturnUnknownFields: p.returnUnknownFields,
		CodeFileLineFunc:    p.caller,
		CorpId:              p.corpId,
		IgnoreBroken:        p.ignoreBroken,
	}
}

func (p *BaseScope[M]) Where(args ...interface{}) *BaseScope[M] {
	p.cond.Where(args...)
	if p.corpId == 0 && p.cond.corpId != 0 {
		p.corpId = p.cond.corpId
	}
	return p
}

func (p *BaseScope[M]) OrWhere(args ...interface{}) *BaseScope[M] {
	p.cond.OrWhere(args...)
	return p
}

func (p *BaseScope[M]) WhereIn(fieldName string, list interface{}) *BaseScope[M] {
	vo := EnsureIsSliceOrArray(list)
	if vo.Len() == 0 {
		p.cond.where(false)
		return p
	}
	p.cond.where(fieldName, "IN", utils.UniqueSliceV2(vo.Interface()))
	return p
}

func (p *BaseScope[M]) WhereNotIn(fieldName string, list interface{}) *BaseScope[M] {
	vo := EnsureIsSliceOrArray(list)
	if vo.Len() == 0 {
		return p
	}
	p.cond.where(fieldName, "NOT IN", utils.UniqueSliceV2(vo.Interface()))
	return p
}

func (p *BaseScope[M]) WhereBetween(fieldName string, min, max interface{}) *BaseScope[M] {
	p.cond.whereRaw(fmt.Sprintf(quoteFieldName(fieldName))+" BETWEEN ? AND ?", min, max)
	return p
}

func (p *BaseScope[M]) WhereNotBetween(fieldName string, min, max interface{}) *BaseScope[M] {
	p.cond.whereRaw(fmt.Sprintf(quoteFieldName(fieldName))+" NOT BETWEEN ? AND ?", min, max)
	return p
}

func (p *BaseScope[M]) WhereLike(fieldName string, str string) *BaseScope[M] {
	p.cond.conds = append(p.cond.conds, fmt.Sprintf("%s LIKE %s", quoteFieldName(fieldName), quoteStr(utils.EscapeMysqlLikeWildcardIgnore2End(str))))
	return p
}

func (p *BaseScope[M]) WhereNotLike(fieldName string, str string) *BaseScope[M] {
	p.cond.conds = append(p.cond.conds, fmt.Sprintf("%s NOT LIKE %s", quoteFieldName(fieldName), quoteStr(utils.EscapeMysqlLikeWildcardIgnore2End(str))))
	return p
}

func (p *BaseScope[M]) Unscoped(b ...bool) *BaseScope[M] {
	if len(b) == 0 {
		p.unscoped = true
		return p
	}
	p.unscoped = b[0]
	return p
}

func (p *BaseScope[M]) Create(ctx uctx.IUCtx, obj interface{}) error {
	j, err := toJonSkipZeroValueField(obj)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	ormEngine := engine.GetOrmEngine()
	rsp, err := ormEngine.InsertModel(ctx, &engine.InsertModelReq{
		ObjType:          p.m.modelType,
		Db:               p.db,
		Table:            p.GetTableName(),
		JsonData:         j,
		Skips:            p.skips,
		CodeFileLineFunc: p.caller,
		CorpId:           p.corpId,
		IgnoreBroken:     p.ignoreBroken,
	})
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}
	if rsp.JsonData != "" {
		if m, ok := obj.(map[string]interface{}); ok {
			err = sonic.Unmarshal([]byte(rsp.JsonData), &m)
			if err != nil {
				log.Errorf("err:%s", err)
				return err
			}
		} else {
			err = sonic.Unmarshal([]byte(rsp.JsonData), obj)
			if err != nil {
				log.Errorf("err:%s", err)
				return err
			}
		}
	}
	return nil
}

func (p *BaseScope[M]) FirstWithResult(ctx uctx.IUCtx, obj interface{}) (SelectResult, error) {
	if p.cond.skipRpc {
		return SelectResult{}, p.m.GetNotFoundErr()
	}

	var res SelectResult
	p.limit = 1
	p.offset = 0
	req := p.newGetModelListReq()
	ormEngine := engine.GetOrmEngine()
	rsp, err := ormEngine.GetModelList(ctx, req)
	if err != nil {
		log.Errorf("err:%s", err)
		return res, err
	}
	if rsp.RowsJson == "" {
		return res, p.m.GetNotFoundErr()
	}
	if err = rowsJsonToPb(rsp.RowsJson, obj); err != nil {
		return res, err
	}
	res.Total = rsp.Total
	return res, nil
}

func (p *BaseScope[M]) First(ctx uctx.IUCtx) (M, error) {
	var obj M
	_, err := p.FirstWithResult(ctx, &obj)
	return obj, err
}

func (p *BaseScope[M]) FindWithResult(ctx uctx.IUCtx, out interface{}) (SelectResult, error) {
	if p.cond.skipRpc {
		return SelectResult{}, nil
	}

	var res SelectResult
	v := reflect.ValueOf(out)
	if v.Type().Kind() != reflect.Ptr {
		err := errors.New("invalid out type, not ptr")
		return res, err
	}
	v = v.Elem()
	if v.Type().Kind() != reflect.Slice {
		err := errors.New("invalid out type, not ptr to slice")
		return res, err
	}

	req := p.newGetModelListReq()
	ormEngine := engine.GetOrmEngine()
	rsp, err := ormEngine.GetModelList(ctx, req)
	if err != nil {
		log.Errorf("err:%s", err)
		return res, err
	}
	if rsp.RowsJson != "" {
		err = sonic.Unmarshal([]byte(rsp.RowsJson), out)
		if err != nil {
			log.Errorf("err:%s", err)
			return res, err
		}
	} else {
		ClearSlice(out)
	}
	res.Total = rsp.Total
	res.NextOffset = rsp.NextOffset
	res.CorpId = rsp.CorpId
	return res, nil
}

func (p *BaseScope[M]) Find(ctx uctx.IUCtx) ([]M, error) {
	var out []M
	_, err := p.FindWithResult(ctx, &out)
	return out, err
}

func (p *BaseScope[M]) Delete(ctx uctx.IUCtx) (DeleteResult, error) {
	if p.cond.skipRpc {
		return DeleteResult{}, nil
	}
	var res DeleteResult
	cond := p.cond.ToString()
	if cond == "" {
		return res, errors.New("cond empty")
	}
	ormEngine := engine.GetOrmEngine()
	req := &engine.DelModelReq{
		ObjType:          p.m.modelType,
		Table:            p.GetTableName(),
		Cond:             cond,
		Unscoped:         p.unscoped,
		Limit:            p.limit,
		CodeFileLineFunc: p.caller,
		CorpId:           p.corpId,
		IgnoreBroken:     p.ignoreBroken,
		Db:               p.db,
	}
	rsp, err := ormEngine.DelModel(ctx, req)
	if err != nil {
		log.Errorf("err:%s", err)
		return res, err
	}
	res.RowsAffected = rsp.RowsAffected
	return res, nil
}

// Updates 支持map和model更新,更新model字段非零值,注意bool类型
func (p *BaseScope[M]) Updates(ctx uctx.IUCtx, m interface{}) (UpdateResult, error) {
	if v, ok := m.(map[string]interface{}); ok {
		return p.Update(ctx, v)
	}
	mVal := reflect.ValueOf(m)
	if mVal.Type().Kind() == reflect.Ptr {
		mVal = mVal.Elem()
	}
	mType := mVal.Type()
	var res UpdateResult
	if mType.Kind() != reflect.Struct {
		err := fmt.Errorf("dbx.updates.argv[1] must be map or struct")
		return res, err
	}
	valMap := make(map[string]interface{})
	fieldNum := mType.NumField()
	for i := 0; i < fieldNum; i++ {
		fieldType := mType.Field(i)
		fieldVal := mVal.Field(i)
		if fieldVal.IsValid() && fieldVal.CanInterface() && !fieldVal.IsZero() {
			valMap[utils.Camel2UnderScore(fieldType.Name)] = fieldVal.Interface()
		}
	}
	if len(valMap) < 1 {
		err := fmt.Errorf("dbx.updates.argv[1] not found update field")
		return res, err
	}
	return p.Update(ctx, valMap)
}

func (p *BaseScope[M]) Update(ctx uctx.IUCtx, updateMap map[string]interface{}) (UpdateResult, error) {
	if p.cond.skipRpc {
		return UpdateResult{}, nil
	}
	var res UpdateResult
	if len(updateMap) == 0 {
		err := errors.New("update map is empty")
		return res, err
	}
	cond := p.cond.ToString()
	if cond == "" {
		err := errors.New("cond is empty")
		return res, err
	}
	buf, err := sonic.Marshal(updateMap)
	if err != nil {
		log.Errorf("err:%s", err)
		return res, err
	}
	j := string(buf)
	ormEngine := engine.GetOrmEngine()
	rsp, err := ormEngine.UpdateModel(ctx, &engine.UpdateModelReq{
		ObjType:          p.m.modelType,
		Table:            p.GetTableName(),
		JsonData:         j,
		Cond:             cond,
		Skips:            p.skips,
		Unscoped:         p.unscoped,
		CodeFileLineFunc: p.caller,
		CorpId:           p.corpId,
		Limit:            p.limit,
		IgnoreBroken:     p.ignoreBroken,
		Db:               p.db,
	})
	if err != nil {
		log.Errorf("err:%s", err)
		return res, err
	}

	res.RowsAffected = rsp.RowsAffected
	return res, nil
}

func (p *BaseScope[M]) BatchCreate(ctx uctx.IUCtx, chunkSize int, objList interface{}) (BatchCreateResult, error) {
	var res BatchCreateResult

	// 如果传入的 list 太长，要切成一块块 batch insert，否则 db 那边要GG
	if chunkSize <= 0 {
		chunkSize = 1024
	}

	vo := reflect.ValueOf(objList)
	if vo.Kind() != reflect.Slice && vo.Kind() != reflect.Array {
		panic(fmt.Sprintf("invalid objList type %v, required slice or array", vo.Type()))
	}
	if vo.Len() == 0 {
		return res, nil
	}

	if vo.Len() > chunkSize {
		log.Warnf("batch insert len %d, too big, split to chunk with size %d",
			vo.Len(), chunkSize)
	}
	for chunkIdx := 0; chunkIdx < vo.Len(); chunkIdx += chunkSize {
		var list []interface{}
		for i := 0; i < chunkSize && chunkIdx+i < vo.Len(); i++ {
			list = append(list, vo.Index(chunkIdx+i).Interface())
		}
		var jsonList []string
		for _, obj := range list {
			j, err := toJonSkipZeroValueField(obj)
			if err != nil {
				log.Errorf("err:%s", err)
				return res, err
			}
			jsonList = append(jsonList, j)
		}
		ormEngine := engine.GetOrmEngine()
		rsp, err := ormEngine.BatchInsertModel(ctx, &engine.BatchInsertModelReq{
			ObjType:          p.m.modelType,
			Table:            p.GetTableName(),
			JsonDataList:     jsonList,
			IgnoreConflict:   p.ignoreConflict,
			CodeFileLineFunc: p.caller,
			CorpId:           p.corpId,
			IgnoreBroken:     p.ignoreBroken,
			Db:               p.db,
		})
		if err != nil {
			log.Errorf("err:%s", err)
			return res, err
		}
		id := rsp.LastInsertId
		for _, obj := range list {
			v := reflect.ValueOf(obj)
			for v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			if v.Kind() == reflect.Struct {
				f := v.FieldByName("Id")
				if f.IsValid() {
					if f.Kind() == reflect.Uint64 || f.Kind() == reflect.Uint32 {
						if f.Uint() == 0 {
							f.SetUint(id)
							id++
						}
					}
				}
			}
		}
		res.RowsAffected += rsp.GetRowsAffected()
	}
	return res, nil
}

func (p *BaseScope[M]) Save(ctx uctx.IUCtx, obj interface{}) (UpdateResult, error) {
	var res UpdateResult
	var j string
	var err error
	if pb, ok := obj.(proto.Message); ok {
		j, err = Pb2JsonDoNotSkipDefaults(pb)
		if err != nil {
			log.Errorf("err:%v", err)
			return res, err
		}
	} else {
		buf, err := sonic.Marshal(obj)
		if err != nil {
			log.Errorf("err:%v", err)
			return res, err
		}
		j = string(buf)
	}
	cond := p.cond.ToString()

	ormEngine := engine.GetOrmEngine()
	rsp, err := ormEngine.SetModel(ctx, &engine.SetModelReq{
		ObjType:          p.m.modelType,
		Table:            p.GetTableName(),
		JsonData:         j,
		Skips:            p.skips,
		Unscoped:         p.unscoped,
		Cond:             cond,
		CodeFileLineFunc: p.caller,
		CorpId:           p.corpId,
		IgnoreBroken:     p.ignoreBroken,
		Db:               p.db,
	})
	if err != nil {
		log.Errorf("err:%s", err)
		return res, err
	}
	if rsp.JsonData != "" {
		err = sonic.Unmarshal([]byte(rsp.JsonData), obj)
		if err != nil {
			log.Errorf("err:%s, json %s, pb %s", err, rsp.JsonData, p.m.modelType)
			return res, err
		}
	}
	res.RowsAffected = rsp.RowsAffected
	return res, nil
}

type numType struct {
	Out uint64 `json:"out"`
}

// Count `count` 操作，注意，调用本方法时会清空select字段
// field 仅支持第一个字段，如果有多个字段，只取第一个
func (p *BaseScope[M]) Count(ctx uctx.IUCtx, field ...string) (count uint64, err error) {
	if p.cond.skipRpc {
		return 0, nil
	}
	p.CleanSelect()

	if len(field) > 0 {
		p.Select(fmt.Sprintf("COUNT(%s) as count", field[0]))
	} else {
		p.Select("COUNT(*) as count")
	}

	var c numType
	_, err = p.FirstWithResult(ctx, &c)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	count = c.Out
	return
}

// DistinctCount `distinct count` 操作，注意，调用本方法时会清空select字段
// field 仅支持第一个字段，如果有多个字段，只取第一个
func (p *BaseScope[M]) DistinctCount(ctx uctx.IUCtx, field ...string) (count uint64, err error) {
	if p.cond.skipRpc {
		return 0, nil
	}
	p.CleanSelect()

	if len(field) > 0 {
		p.Select(fmt.Sprintf("COUNT(distinct %s) as count", field[0]))
	} else {
		p.Select("COUNT(distinct *) as count")
	}

	var c numType
	_, err = p.FirstWithResult(ctx, &c)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	count = c.Out
	return
}

// Sum `sum` 操作，注意，调用本方法时会清空select字段
// field 仅支持第一个字段，如果有多个字段，只取第一个
func (p *BaseScope[M]) Sum(ctx uctx.IUCtx, field ...string) (sum uint64, err error) {
	if p.cond.skipRpc {
		return 0, nil
	}

	p.CleanSelect()

	if len(field) > 0 {
		p.Select(fmt.Sprintf("SUM(%s) as out", field[0]))
	} else {
		p.Select("SUM(*) as out")
	}

	var s numType
	_, err = p.FirstWithResult(ctx, &s)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	sum = s.Out
	return
}

func (p *BaseScope[M]) IsEmpty(ctx uctx.IUCtx) (res bool, err error) {
	p.limit = 1
	p.CleanSelect().Select("id")
	var c numType
	_, err = p.FirstWithResult(ctx, &c)
	if err != nil {
		if err == p.m.GetNotFoundErr() {
			return true, nil
		}
		log.Errorf("err:%v", err)
		return
	}

	return
}

func Expr(expression string, args ...interface{}) map[string]any {
	return map[string]any{
		"expr": expression,
		"args": args,
	}
}

func (p *BaseScope[M]) FindPaginate(ctx uctx.IUCtx, out interface{}) (*core.Paginate, error) {
	res, err := p.FindWithResult(ctx, out)
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, err
	}
	o := &core.Paginate{
		Offset: p.offset,
		Limit:  p.limit,
		Total:  res.Total,
	}
	return o, nil
}

func (p *BaseScope[M]) Chunk(ctx uctx.IUCtx, chunkSize uint32, callback func(out []M) error) error {
	if p.cond.skipRpc {
		return nil
	}
	offset := uint32(0)
	p.NeedCount(false).Limit(chunkSize)

	for {
		var out []M
		_, err := p.Offset(offset).FindPaginate(ctx, &out)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if len(out) == 0 {
			break
		}
		err = callback(out)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		offset += chunkSize
	}
	return nil
}

func (p *BaseScope[M]) UpdateOrCreate(ctx uctx.IUCtx, attributes map[string]interface{}, values map[string]interface{}) (UpdateOrCreateResult[M], error) {
	if p.cond.skipRpc {
		return UpdateOrCreateResult[M]{}, nil
	}

	var res UpdateOrCreateResult[M]
	var m map[string]interface{}
	_, err := p.Where(attributes).FirstWithResult(ctx, &m)
	if err != nil {
		if !p.m.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return res, err
		}
		// create
		res.Created = true
		all := map[string]interface{}{}
		for k, v := range values {
			all[k] = v
		}
		for k, v := range attributes {
			all[k] = v
		}
		err = p.Create(ctx, all)
		if err != nil {
			log.Errorf("err:%v", err)
			return res, err
		}
		m = all
	} else {
		// values
		upRes, err := p.Update(ctx, values)
		if err != nil {
			log.Errorf("err:%v", err)
			return res, err
		}
		for k, v := range values {
			m[k] = v
		}
		res.RowsAffected = upRes.RowsAffected
	}
	err = map2Interface(m, &res.Object)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}
	return res, nil
}

func (p *BaseScope[M]) FirstOrCreate(ctx uctx.IUCtx, attributes map[string]interface{}, values map[string]interface{}) (FirstOrCreateResult[M], error) {
	var res FirstOrCreateResult[M]
	var m map[string]interface{}

	_, err := p.Where(attributes).FirstWithResult(ctx, &m)
	if err != nil {
		if !p.m.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return res, err
		}
		all := map[string]interface{}{}
		for k, v := range values {
			all[k] = v
		}
		for k, v := range attributes {
			all[k] = v
		}
		err = p.Create(ctx, all)
		if err != nil {
			if lberr.GetErrCode(err) == ErrModelUniqueIndexConflict {
				_, err2 := p.Where(attributes).FirstWithResult(ctx, &m)
				if err2 != nil {
					if p.m.IsNotFoundErr(err2) {
						// create 失败，但是 get 不到，要报 conflict
						return res, err
					}
					log.Errorf("err:%v", err2)
					return res, err2
				}
			} else {
				log.Errorf("err:%v", err)
				return res, err
			}
		} else {
			// create
			res.Created = true
			m = all
		}
	}
	err = map2Interface(m, &res.Object)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}
	return res, nil
}
