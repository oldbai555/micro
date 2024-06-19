/**
 * @Author: zjj
 * @Date: 2024/6/19
 * @Desc:
**/

package egimpl

import (
	jsonIter "github.com/json-iterator/go"
)

// 这里的 json 解包，对于 number 类型有点坑
// 这里用 UseNumber 去读，避免转成 float64 丢失精度
var jsonEx = jsonIter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	UseNumber:              true,
}.Froze()

func decodeJson(s string, v interface{}) error {
	return jsonEx.UnmarshalFromString(s, v)
}
