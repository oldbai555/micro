/**
 * @Author: zjj
 * @Date: 2025/1/9
 * @Desc:
**/

package gormx

import (
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/micro/gormx/engine"
	"github.com/oldbai555/micro/uctx"
)

func OnTransaction(ctx uctx.IUCtx, f func(ctx uctx.IUCtx, trId string) error) error {
	ormEngine := engine.GetOrmEngine()
	trId, err := ormEngine.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			log.Errorf("recoverErr:%v", recoverErr)
			deferErr := ormEngine.Rollback(trId)
			if deferErr != nil {
				log.Errorf("deferErr:%v", deferErr)
			}
			return
		}
		if err != nil {
			deferErr := ormEngine.Rollback(trId)
			if deferErr != nil {
				log.Errorf("deferErr:%v", deferErr)
			}
			return
		}
		deferErr := ormEngine.Commit(trId)
		if deferErr != nil {
			log.Errorf("deferErr:%v", deferErr)
		}
	}()
	if err = f(ctx, trId); err != nil {
		return err
	}
	return nil
}
