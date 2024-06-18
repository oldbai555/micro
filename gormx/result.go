package gormx

type DeleteResult struct {
	RowsAffected uint64
}

type UpdateResult struct {
	RowsAffected uint64
	Sql          string
}

type SelectResult struct {
	Total      uint32
	NextOffset uint32
	CorpId     uint32
}

type UpdateOrCreateResult[M any] struct {
	Object       M
	Created      bool
	RowsAffected uint64
}

type FirstOrCreateResult[M any] struct {
	Object  M
	Created bool
}

type BatchCreateResult struct {
	RowsAffected uint64
}
