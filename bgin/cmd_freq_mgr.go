package bgin

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/lbtool/pkg/lberr"
	"github.com/oldbai555/lbtool/utils"
	"github.com/oldbai555/micro/bcmd"
	"net/http"
	"strings"
	"sync"
)

type CmdFreqMgr struct {
	CmdList      []*bcmd.Cmd
	FreqLimitMgr *FrequencyRuleMgr
}

func NewCmdFreqMgr(cmdList []*bcmd.Cmd) *CmdFreqMgr {
	return &CmdFreqMgr{CmdList: cmdList, FreqLimitMgr: NewFrequencyRuleMgr()}
}

type FrequencyRuleMgr struct {
	L sync.Mutex
	M map[string]*FrequencyRule
}

type FrequencyRule struct {
	L      sync.Mutex
	MinSec uint32
	MaxSec uint32
	Count  uint32
}

func (m *FrequencyRule) reset() {
	m.MaxSec = 0
	m.MinSec = 0
	m.Count = 0
}

func (m *FrequencyRule) check() error {
	m.L.Lock()
	defer m.L.Unlock()

	nowSec := utils.TimeNow()
	if m.MaxSec == 0 || m.MinSec == 0 {
		m.reset()
		m.MinSec = nowSec
		m.MaxSec = nowSec + 5
	}
	if nowSec > m.MaxSec {
		m.reset()
		m.MinSec = nowSec
		m.MaxSec = nowSec + 5
	}
	if m.MinSec <= nowSec && nowSec <= m.MaxSec {
		m.Count++
	}
	if m.Count > 10 {
		return lberr.NewErr(-1001, "api up limit")
	}
	return nil
}

func NewFrequencyRuleMgr() *FrequencyRuleMgr {
	return &FrequencyRuleMgr{
		M: make(map[string]*FrequencyRule),
	}
}

func (m *FrequencyRuleMgr) check(key string) error {
	m.L.Lock()
	defer m.L.Unlock()

	rule, ok := m.M[key]
	if !ok {
		m.M[key] = &FrequencyRule{}
		rule = m.M[key]
	}
	err := rule.check()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (s *CmdFreqMgr) CheckApiFrequencyLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		var cm *bcmd.Cmd
		handler := NewHandler(c)

		var path = c.Request.RequestURI

		// todo 传入就按传入的过滤 否则按 api 实际过滤
		if len(s.CmdList) > 0 {
			for _, cmd := range s.CmdList {
				if strings.HasSuffix(c.Request.RequestURI, cmd.Path) {
					cm = cmd
					break
				}
			}

			// 找不到
			if cm == nil {
				// 404
				handler.RespByJson(http.StatusNotFound, http.StatusNotFound, "", "not found")
				return
			}
			path = cm.Path
		}

		// 接口限频
		err := s.FreqLimitMgr.check(fmt.Sprintf("%s_%s_%s", c.RemoteIP(), c.ClientIP(), path))
		if err != nil {
			log.Errorf("err:%v", err)
			handler.Error(err)
			return
		}
		c.Next()
	}
}
