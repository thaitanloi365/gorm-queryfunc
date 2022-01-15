package queryfunc

import (
	"gorm.io/gorm"
)

type BuilderFunc = func(builder *Builder)

type Handler = func(db DB, rawSQL *gorm.DB) (interface{}, error)

type Builder struct {
	rawSQL       string
	countRawSQL  string
	orderBy      string
	groupBy      string
	having       string
	isWrapJSON   bool
	paginationFn Handler
}

func NewBuilder(rawSQL string, countRawSQL ...string) *Builder {
	var b = &Builder{
		rawSQL:      rawSQL,
		countRawSQL: "",
		orderBy:     "",
		groupBy:     "",
		isWrapJSON:  false,
	}
	if len(countRawSQL) > 0 {
		b.countRawSQL = countRawSQL[0]
	}
	return b
}

func (b *Builder) WithPaginationFunc(fn Handler) *Builder {
	b.paginationFn = fn
	return b
}

func (b *Builder) WithRawSQL(sql string) *Builder {
	b.rawSQL = sql
	return b
}

func (b *Builder) WithWrapJSON(wrap bool) *Builder {
	b.isWrapJSON = wrap
	return b
}

func (b *Builder) WithCountRawSQL(sql string) *Builder {
	b.countRawSQL = sql
	return b
}

func (b *Builder) WithOrderBy(sql string) *Builder {
	b.orderBy = sql
	return b
}

func (b *Builder) WithGroupBy(sql string) *Builder {
	b.groupBy = sql
	return b
}

func (b *Builder) WithHaving(sql string) *Builder {
	b.having = sql
	return b
}

/* Implement interface */
func (b *Builder) GetRawSQL() string {
	return b.rawSQL
}

func (b *Builder) GetCountRawSQL() string {
	return b.countRawSQL
}

func (b *Builder) GetGroupByRawSQL() string {
	return b.groupBy
}

func (b *Builder) GetOrderByRawSQL() string {
	return b.orderBy
}

func (b *Builder) GetHavingRawSQL() string {
	return b.having
}

func (b *Builder) IsWrapJSON() bool {
	return b.isWrapJSON
}

func (b *Builder) GetPaginationFunc() Handler {
	return b.paginationFn
}
