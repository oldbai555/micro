/**
 * @Author: zjj
 * @Date: 2024/3/26
 * @Desc:
**/

package main

import (
	"context"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/micro"
)

func main() {
	grpcWithGateSrv := micro.NewGrpcWithGateSrv("testSrv", "", 2001)
	err := grpcWithGateSrv.Start(context.Background())
	if err != nil {
		log.Warnf("err:%v", err)
		return
	}
}
