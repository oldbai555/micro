/**
 * @Author: zjj
 * @Date: 2024/6/18
 * @Desc:
**/

package egimpl

import (
	"fmt"
	"github.com/oldbai555/micro/gormx/engine"
	"strings"
)

func findFieldsByGetModelListReq(req *engine.GetModelListReq) (string, error) {
	// var fields string
	if len(req.Fields) > 0 {
		addId := true

		q := make([]string, 0, len(req.Fields)+1)
		for _, v := range req.Fields {
			fieldName := quoteName(v)
			if fieldName == "id" {
				addId = false
			}
			q = append(q, fieldName)
		}
		if addId {
			q = append(q, "id")
		}
		return strings.Join(q, ","), nil
	}

	return "*", nil
}

func quoteName(name string) string {
	if name != "" {
		if name[0] != '`' {
			q := true
			l := len(name)
			for i := 0; i < l; i++ {
				c := name[i]
				if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
					q = false
					break
				}
			}
			if q {
				name = fmt.Sprintf("`%s`", name)
			}
		}
	}
	return name
}

func hasDeletedAtField() bool {
	return false
}

type Rows struct {
	cols   []string
	colMap map[string]int
	rows   [][]string
}
