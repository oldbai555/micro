/**
 * @Author: zjj
 * @Date: 2024/6/18
 * @Desc:
**/

package egimpl

import (
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/micro/gormx/engine"
	"github.com/oldbai555/micro/uctx"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	autoMigrateOptKey   = "bdb:table_options"
	autoMigrateOptValue = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
)

var _ engine.IOrmEngine = (*GormEngine)(nil)

type GormEngine struct {
	db         *gorm.DB
	lock       sync.Mutex
	objTypeMgr map[string]*engine.ModelObjectType
}

func (g *GormEngine) RegObjectType(objTypeList ...*engine.ModelObjectType) {
	g.lock.Lock()
	defer g.lock.Unlock()
	for _, objectType := range objTypeList {
		g.objTypeMgr[objectType.Name] = objectType
	}
}

func (g *GormEngine) DB() *gorm.DB {
	return g.db
}

func (g *GormEngine) GetModelList(ctx uctx.IUCtx, req *engine.GetModelListReq) (*engine.GetModelListRsp, error) {
	var rsp engine.GetModelListRsp

	objType, ok := g.objTypeMgr[req.ObjType]
	if !ok {
		panic(fmt.Sprintf("not found obj type %s", req.ObjType))
	}

	fields, err := findFieldsByGetModelListReq(req, objType)
	if err != nil {
		return nil, err
	}

	var items []string
	items = append(items, fmt.Sprintf("SELECT %s FROM %s", fields, quoteName(req.Table)))
	hasDeletedAt := hasDeletedAtField(objType)
	if hasDeletedAt && !req.Unscoped {
		if req.Cond != "" {
			req.Cond += " AND "
		}
		req.Cond += "(deleted_at=0 OR deleted_at IS NULL)"
	}

	if req.Cond != "" {
		items = append(items, fmt.Sprintf("WHERE %s", req.Cond))
	}

	if req.Group != "" {
		items = append(items, fmt.Sprintf("GROUP BY %s", req.Group))
	}

	if req.Order != "" {
		items = append(items, fmt.Sprintf("ORDER BY %s", req.Order))
	}

	if req.Limit > 0 {
		items = append(items, fmt.Sprintf("LIMIT %d", req.Limit))
	}

	if req.Offset > 0 {
		items = append(items, fmt.Sprintf("OFFSET %d", req.Offset))
	}

	res, err := RawQuery(ctx, g.db, strings.Join(items, " "))
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	list := rawResToListMap(objType, res, req.ReturnUnknownFields)

	rsp.NextOffset = req.Offset + req.Limit
	rsp.CorpId = req.CorpId

	// count 一下
	if !req.SkipCount && req.Offset == 0 && req.Limit > 0 {
		// 如果结果超过了一页，进行count*，否则返回当前页的总结果数
		if uint32(len(res.rows)) >= req.Limit {
			items = nil
			items = append(items, fmt.Sprintf("SELECT COUNT(*) AS total FROM %s", quoteName(req.Table)))
			if req.Cond != "" {
				items = append(items, fmt.Sprintf("WHERE %s", req.Cond))
			}
			if req.Group != "" {
				items = append(items, fmt.Sprintf("GROUP BY %s", req.Group))
			}
			stmt := strings.Join(items, " ")
			res, err := RawQuery(ctx, g.db, stmt)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			if len(res.rows) == 0 {
				return nil, lberr.NewErr(-1, "empty response")
			}
			row := res.rows[0]
			if len(row) == 0 {
				return nil, lberr.NewErr(-1, "empty row")
			}
			if req.Group != "" {
				rsp.Total = uint32(len(res.rows))
			} else {
				x, err := strconv.ParseInt(row[0], 10, 32)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				rsp.Total = uint32(x)
			}
		} else {
			rsp.Total = uint32(len(res.rows))
		}
	}

	// 转义一下返回
	j, err := sonic.MarshalString(list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.RowsJson = j
	return &rsp, nil
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
	g := &GormEngine{
		objTypeMgr: make(map[string]*engine.ModelObjectType),
	}
	ormLog := NewOrmLog(time.Second * 5)
	ormLog.skipCall = 11
	g.db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,  // 是否单表，命名是否复数
			NoLowerCase:   false, // 是否关闭驼峰命名
		},

		PrepareStmt: true, // 预编译 在执行任何 SQL 时都会创建一个 prepared statement 并将其缓存，以提高后续的效率

		Logger: ormLog,
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
