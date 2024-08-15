/**
 * @Author: zjj
 * @Date: 2024/7/25
 * @Desc:
**/

package egimpl

import (
	"fmt"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/utils"
	"github.com/oldbai555/micro/gormx/engine"
	"github.com/oldbai555/micro/uctx"
	"gorm.io/gorm"
)

func (g *GormEngine) DelModel(ctx uctx.IUCtx, req *engine.DelModelReq) (*engine.DelModelRsp, error) {
	var rsp engine.DelModelRsp

	objType, ok := g.objTypeMgr[req.ObjType]
	if !ok {
		panic(fmt.Sprintf("not found obj type %s", req.ObjType))
	}

	hasDeletedAt := hasDeletedAtField(objType)
	if hasDeletedAt && !req.Unscoped {
		if req.Cond != "" {
			req.Cond += " AND "
		}
		req.Cond += "(deleted_at=0 OR deleted_at IS NULL)"
	}

	var limitCond string
	if req.Limit > 0 {
		limitCond = fmt.Sprintf(" LIMIT %d", req.Limit)
	}

	var res *gorm.DB
	var err error
	if hasDeletedAt && !req.Unscoped {
		now := utils.TimeNow()
		res = dbExec(
			ctx, g.db,
			fmt.Sprintf("UPDATE %s SET deleted_at=%d WHERE %s%s",
				quoteName(req.Table), now, req.Cond, limitCond))
		err = res.Error
		if err != nil {
			log.Errorf("err:%s", err)
			return nil, err
		}
	} else {
		res = dbExec(
			ctx, g.db,
			fmt.Sprintf("DELETE FROM %s WHERE %s%s",
				quoteName(req.Table), req.Cond, limitCond), Option{codeFileLineFunc: req.CodeFileLineFunc, ignoreBroken: req.IgnoreBroken})
		err = res.Error
		if err != nil {
			log.Errorf("err:%s", err)
			return nil, err
		}
	}

	rsp.RowsAffected = uint64(res.RowsAffected)
	return &rsp, nil

	return &rsp, nil
}
