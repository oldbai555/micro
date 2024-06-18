package gormx

import (
	"github.com/oldbai555/lbtool/log"
	"github.com/oldbai555/micro/core"
)

func ProcessDefaultOptions(listOption *core.ListOption, db *BaseScope[any]) error {
	err := core.NewOptionsProcessor(listOption).
		AddStringList(
			core.DefaultListOption_DefaultListOptionSelect,
			func(valList []string) error {
				db.Select(valList...)
				return nil
			}).
		AddUint32(
			core.DefaultListOption_DefaultListOptionOrderBy,
			func(val uint32) error {
				if val == uint32(core.DefaultOrderBy_DefaultOrderByCreatedAtDesc) {
					db.OrderDesc("created_at")
				} else if val == uint32(core.DefaultOrderBy_DefaultOrderByCreatedAtAcs) {
					db.OrderAsc("created_at")
				} else if val == uint32(core.DefaultOrderBy_DefaultOrderByIdDesc) {
					db.OrderDesc("id")
				}
				return nil
			}).
		AddStringList(
			core.DefaultListOption_DefaultListOptionGroupBy,
			func(valList []string) error {
				db.Group(valList...)
				return nil
			}).
		AddBool(
			core.DefaultListOption_DefaultListOptionWithTrash,
			func(val bool) error {
				if val {
					db.Unscoped()
				}
				return nil
			}).
		AddUint64List(
			core.DefaultListOption_DefaultListOptionIdList,
			func(valList []uint64) error {
				if len(valList) == 1 {
					db.Where("id", valList[0])
				} else {
					db.WhereIn("id", valList)
				}
				return nil
			}).
		AddTimeStampRange(
			core.DefaultListOption_DefaultListOptionCreatedAt,
			func(begin, end uint32) error {
				db.WhereBetween("created_at", begin, end)
				return nil
			}).
		AddUint32List(
			core.DefaultListOption_DefaultListOptionCreatorIdList,
			func(valList []uint32) error {
				db.WhereIn("creator_id", valList)
				return nil
			}).
		AddUint64List(
			core.DefaultListOption_DefaultListOptionCorpIdList,
			func(valList []uint64) error {
				if len(valList) == 1 {
					db.Where("corp_id", valList[0])
				} else {
					db.WhereIn("corp_id", valList)
				}
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}
