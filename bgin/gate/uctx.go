package gate

import (
	"github.com/gin-gonic/gin"
	"github.com/oldbai555/micro/uctx"
)

var _ uctx.IUCtx = (*GinUCtx)(nil)

func NewGinUCtx(ctx *gin.Context) *GinUCtx {
	return &GinUCtx{
		Context:  ctx,
		BaseUCtx: uctx.NewBaseUCtx(),
	}
}

type GinUCtx struct {
	*gin.Context
	*uctx.BaseUCtx
}
