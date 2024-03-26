package bdb

import (
	"context"
	"github.com/oldbai555/gorm"
	"github.com/oldbai555/lbtool/pkg/gormx"
)

type Scope struct {
	*gormx.Scope

	size      uint32
	page      uint32
	skipTotal bool
}

type Model struct {
	*gormx.Model
}

func NewModel(db *gorm.DB, m gorm.Tabler, err error) *Model {
	return &Model{
		Model: gormx.NewModel(db, m, err),
	}
}

func (f *Model) NewScope(ctx context.Context) *Scope {
	return &Scope{
		Scope: f.Model.NewScope(ctx),
	}
}

func (p *Scope) Corp(corpId uint32) *Scope {
	p.Eq("corp_id", corpId)
	return p
}
