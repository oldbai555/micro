package bdb

import (
	mysql "github.com/oldbai555/driver-mysql"
	"github.com/oldbai555/gorm"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/gormx"
	"time"
)

const (
	autoMigrateOptKey   = "bdb:table_options"
	autoMigrateOptValue = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
)

var MasterOrm *gorm.DB
var modelList []interface{}

func RegisterModel(vs ...interface{}) {
	modelList = append(modelList, vs...)
}

func InitMasterOrm(dsn string) error {
	var err error
	MasterOrm, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: gorm.NamingStrategy{
			SingularTable: true,  // 是否单表，命名是否复数
			NoLowerCase:   false, // 是否关闭驼峰命名
		},

		NowFunc: func() int32 {
			return int32(time.Now().Unix())
		},

		PrepareStmt: true, // 预编译 在执行任何 SQL 时都会创建一个 prepared statement 并将其缓存，以提高后续的效率

		Logger: gormx.NewOrmLog(time.Second * 5),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 获取通用数据库对象 sql.DB ，然后使用其提供的功能
	sqlDB, err := MasterOrm.DB()

	// SetMaxIdleConns 用于设置连接池中空闲连接的最大数量。
	sqlDB.SetMaxIdleConns(10)

	// SetMaxOpenConns 设置打开数据库连接的最大数量。
	sqlDB.SetMaxOpenConns(100)

	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	sqlDB.SetConnMaxLifetime(time.Hour)

	return nil
}

func AutoMigrate() {
	if len(modelList) > 0 {
		err := MasterOrm.Set(autoMigrateOptKey, autoMigrateOptValue).AutoMigrate(modelList...)
		if err != nil {
			log.Errorf("err:%v", err)
			panic(err)
		}
	}
}
