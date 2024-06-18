/**
 * @Author: zjj
 * @Date: 2024/6/18
 * @Desc:
**/

package egimpl

import (
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/micro/gormx/engine"
	"github.com/oldbai555/micro/uctx"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

const (
	autoMigrateOptKey   = "bdb:table_options"
	autoMigrateOptValue = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
)

var _ engine.IOrmEngine = (*GormEngine)(nil)

type GormEngine struct {
	db *gorm.DB
}

func (g *GormEngine) DB() *gorm.DB {
	return g.db
}

func (g *GormEngine) GetModelList(ctx uctx.IUCtx, req *engine.GetModelListReq) (*engine.GetModelListRsp, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GormEngine) InsertModel(ctx uctx.IUCtx, req *engine.InsertModelReq) (*engine.InsertModelRsp, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GormEngine) DelModel(ctx uctx.IUCtx, req *engine.DelModelReq) (*engine.DelModelRsp, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GormEngine) UpdateModel(ctx uctx.IUCtx, req *engine.UpdateModelReq) (*engine.UpdateModelRsp, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GormEngine) BatchInsertModel(ctx uctx.IUCtx, req *engine.BatchInsertModelReq) (*engine.BatchInsertModelRsp, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GormEngine) SetModel(ctx uctx.IUCtx, req *engine.SetModelReq) (*engine.SetModelRsp, error) {
	//TODO implement me
	panic("implement me")
}

func (g *GormEngine) AutoMigrate(modelList []interface{}) {
	if len(modelList) == 0 {
		return
	}
	if g.db == nil {
		panic("db is nil")
	}
	err := g.db.Set(autoMigrateOptKey, autoMigrateOptValue).AutoMigrate(modelList...)
	if err != nil {
		log.Errorf("err:%v", err)
		panic(err)
	}
}

func NewGormEngine(dsn string) *GormEngine {
	var err error
	g := &GormEngine{}
	g.db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,  // 是否单表，命名是否复数
			NoLowerCase:   false, // 是否关闭驼峰命名
		},

		PrepareStmt: true, // 预编译 在执行任何 SQL 时都会创建一个 prepared statement 并将其缓存，以提高后续的效率

		Logger: NewOrmLog(time.Second * 5),
	})
	if err != nil {
		panic(err)
	}

	// 获取通用数据库对象 sql.DB ，然后使用其提供的功能
	sqlDB, err := g.db.DB()

	// SetMaxIdleConns 用于设置连接池中空闲连接的最大数量。
	sqlDB.SetMaxIdleConns(10)

	// SetMaxOpenConns 设置打开数据库连接的最大数量。
	sqlDB.SetMaxOpenConns(100)

	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	sqlDB.SetConnMaxLifetime(time.Hour)
	return g
}
