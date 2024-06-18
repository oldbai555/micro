/**
 * @Author: zjj
 * @Date: 2024/6/18
 * @Desc:
**/

package engine

import "github.com/oldbai555/micro/uctx"

type IOrmEngine interface {
	GetModelList(ctx uctx.IUCtx, req *GetModelListReq) (*GetModelListRsp, error)
	InsertModel(ctx uctx.IUCtx, req *InsertModelReq) (*InsertModelRsp, error)
	DelModel(ctx uctx.IUCtx, req *DelModelReq) (*DelModelRsp, error)
	UpdateModel(ctx uctx.IUCtx, req *UpdateModelReq) (*UpdateModelRsp, error)
	BatchInsertModel(ctx uctx.IUCtx, req *BatchInsertModelReq) (*BatchInsertModelRsp, error)
	SetModel(ctx uctx.IUCtx, req *SetModelReq) (*SetModelRsp, error)
}

var ormEngine IOrmEngine

func SetOrmEngine(val IOrmEngine) {
	ormEngine = val
}

func GetOrmEngine() IOrmEngine {
	if ormEngine == nil {
		panic("orm engine is nil")
	}
	return ormEngine
}
