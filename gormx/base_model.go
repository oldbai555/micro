package gormx

import (
	"bytes"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/micro/core"
	"github.com/oldbai555/micro/uctx"
	"gorm.io/gorm"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
)

const defaultLimit uint32 = 2000

type ModelConfig struct {
	NotFoundErrCode int32
	Db              string
}

type BaseModel[M any] struct {
	ModelConfig
	table       string
	modelType   string
	notFoundErr error
}

func NewBaseModel[M any](c ModelConfig) *BaseModel[M] {
	m := &BaseModel[M]{
		ModelConfig: c,
	}

	var obj M
	typ := reflect.TypeOf(obj)
	if typ.Kind() != reflect.Ptr {
		panic("Type not ptr")
	}

	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		panic("Type not struct")
	}

	m.modelType = getModelType(obj)
	m.table = TryGetTableName(obj)
	return m
}

func (p *BaseModel[any]) GetNotFoundErr() error {
	if p.NotFoundErrCode != 0 {
		if p.notFoundErr == nil {
			p.notFoundErr = lberr.NewErr(p.NotFoundErrCode, "record not found")
		}
		return p.notFoundErr
	}
	return gorm.ErrRecordNotFound
}

func (p *BaseModel[any]) IsNotFoundErr(err error) bool {
	return err == p.GetNotFoundErr()
}

func (p *BaseModel[any]) IsUniqueIndexConflictErr(err error) bool {
	return lberr.GetErrCode(err) == ErrModelUniqueIndexConflict
}

func (p *BaseModel[any]) GetCaller() string {
	var b bytes.Buffer

	var callerName string
	pc, callerFile, callerLine, ok := runtime.Caller(4)
	if ok {
		callerName = runtime.FuncForPC(pc).Name()
	}
	filePath, fileFunc := getPackageName(filepath.Base(callerFile), callerName)

	b.WriteString(filePath)
	b.WriteString(":")
	b.WriteString(strconv.Itoa(callerLine))
	b.WriteString(":")
	b.WriteString(fileFunc)

	return b.String()
}

func (p *BaseModel[M]) NewBaseScope() *BaseScope[M] {
	s := &BaseScope[M]{
		m:      p,
		db:     p.Db,
		caller: p.GetCaller(),
	}

	s.cond.isTopLevel = true
	// 默认返回未定义字段
	s.returnUnknownFields = true
	return s
}

func (p *BaseModel[any]) Unscoped(b ...bool) *BaseScope[any] {
	return p.NewBaseScope().Unscoped(b...)
}

func (p *BaseModel[any]) Select(fields ...string) *BaseScope[any] {
	return p.NewBaseScope().Select(fields...)
}

func (p *BaseModel[any]) Where(whereCond ...interface{}) *BaseScope[any] {
	s := p.NewBaseScope()
	s.cond.Where(whereCond...)
	if s.corpId == 0 && s.cond.corpId != 0 {
		s.corpId = s.cond.corpId
	}
	return s
}

func (p *BaseModel[any]) OrWhere(whereCond ...interface{}) *BaseScope[any] {
	return p.NewBaseScope().OrWhere(whereCond...)
}

func (p *BaseModel[M]) WhereIn(fieldName string, list interface{}) *BaseScope[M] {
	return p.NewBaseScope().WhereIn(fieldName, list)
}

func (p *BaseModel[M]) Create(ctx uctx.IUCtx, obj M) error {
	return p.NewBaseScope().Create(ctx, obj)
}

func (p *BaseModel[M]) First(ctx uctx.IUCtx) (M, error) {
	return p.NewBaseScope().First(ctx)
}

func (p *BaseModel[M]) Find(ctx uctx.IUCtx) ([]M, error) {
	return p.NewBaseScope().Find(ctx)
}

func (p *BaseModel[any]) BatchCreate(ctx uctx.IUCtx, chunkSize int, objList interface{}) (BatchCreateResult, error) {
	return p.NewBaseScope().BatchCreate(ctx, chunkSize, objList)
}

func (p *BaseModel[any]) NewList(listOption *core.ListOption) *BaseScope[any] {
	return p.NewBaseScope().NewList(listOption)
}

func (p *BaseModel[M]) UpdateOrCreate(ctx uctx.IUCtx, attributes map[string]interface{}, values map[string]interface{}) (UpdateOrCreateResult[M], error) {
	return p.NewBaseScope().UpdateOrCreate(ctx, attributes, values)
}

func (p *BaseModel[any]) IgnoreBroken(b ...bool) *BaseScope[any] {
	return p.NewBaseScope().IgnoreBroken(b...)
}

func (p *BaseModel[M]) FirstOrCreate(ctx uctx.IUCtx, attributes map[string]interface{}, values map[string]interface{}) (FirstOrCreateResult[M], error) {
	return p.NewBaseScope().FirstOrCreate(ctx, attributes, values)
}
