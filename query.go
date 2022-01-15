package queryfunc

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

type DB interface {
	GormDB() *gorm.DB
}
type QueryBuilder interface {
	GetRawSQL() string
	GetCountRawSQL() string
	GetGroupByRawSQL() string
	GetOrderByRawSQL() string
	GetHavingRawSQL() string
	GetPaginationFunc() Handler
	IsWrapJSON() bool
}

type WhereFunc = func(builder *config)

type Pagination struct {
	HasNext     bool        `json:"has_next"`
	HasPrev     bool        `json:"has_prev"`
	PerPage     int         `json:"per_page"`
	NextPage    int         `json:"next_page"`
	Page        int         `json:"current_page"`
	PrevPage    int         `json:"prev_page"`
	Offset      int         `json:"offset"`
	Records     interface{} `json:"records"`
	TotalRecord int         `json:"total_record"`
	TotalPage   int         `json:"total_page"`
	Metadata    interface{} `json:"metadata"`
}

type config struct {
	db               DB
	RawSQLString     string
	countRawSQL      string
	limit            int
	page             int
	hasWhere         bool
	whereValues      []interface{}
	namedWhereValues map[string]interface{}
	orderBy          string
	groupBy          string
	having           string
	wrapJSON         bool
	qf               QueryBuilder
}

func New(db DB, qf QueryBuilder) *config {
	var builder = &config{
		db:               db,
		RawSQLString:     qf.GetRawSQL(),
		whereValues:      []interface{}{},
		namedWhereValues: map[string]interface{}{},
		hasWhere:         false,
		orderBy:          qf.GetOrderByRawSQL(),
		groupBy:          qf.GetGroupByRawSQL(),
		wrapJSON:         qf.IsWrapJSON(),
		countRawSQL:      qf.GetCountRawSQL(),
		having:           qf.GetHavingRawSQL(),
		qf:               qf,
	}

	return builder
}

func (c *config) WithWrapJSON(isWrapJSON bool) *config {
	c.wrapJSON = isWrapJSON
	return c
}

func (c *config) Where(query interface{}, args ...interface{}) *config {
	switch value := query.(type) {
	case map[string]interface{}:
		for key, v := range value {
			c.namedWhereValues[key] = v
		}
	case map[string]string:
		for key, v := range value {
			c.namedWhereValues[key] = v
		}
	case sql.NamedArg:
		c.namedWhereValues[value.Name] = value.Value
	default:
		if len(args) > 0 {
			c.whereValues = append(c.whereValues, args...)
		}

		if c.hasWhere {
			c.RawSQLString = fmt.Sprintf("%s AND %v", c.RawSQLString, query)
			if c.countRawSQL != "" {
				c.countRawSQL = fmt.Sprintf("%s AND %v", c.countRawSQL, query)
			}
		} else {
			c.RawSQLString = fmt.Sprintf("%s WHERE %v", c.RawSQLString, query)
			if c.countRawSQL != "" {
				c.countRawSQL = fmt.Sprintf("%s WHERE %v", c.countRawSQL, query)
			}
			c.hasWhere = true

		}

	}

	return c
}

// OrderBy specify order when retrieve records from database
func (c *config) OrderBy(orderBy ...string) *config {
	if len(orderBy) > 0 {
		c.orderBy = strings.Join(orderBy, ",")
	}
	return c
}

func (c *config) GroupBy(groupBy string) *config {
	c.groupBy = groupBy
	return c
}

func (c *config) WhereFunc(f WhereFunc) *config {
	f(c)
	return c
}

func (c *config) Limit(limit int) *config {
	c.limit = limit
	return c
}

func (c *config) Page(page int) *config {
	c.page = page
	return c
}

func (c *config) build() (queryString string, countQuery string) {
	var rawSQLString = c.RawSQLString
	queryString = rawSQLString
	countQuery = c.countRawSQL

	if countQuery == "" {
		countQuery = rawSQLString
	}

	if c.groupBy != "" {
		queryString = fmt.Sprintf("%s GROUP BY %s", queryString, c.groupBy)
		countQuery = fmt.Sprintf("%s GROUP BY %s", countQuery, c.groupBy)
	}

	if c.having != "" {
		queryString = fmt.Sprintf("%s HAVING %s", queryString, c.having)
		countQuery = fmt.Sprintf("%s HAVING %s", countQuery, c.having)
	}

	if c.orderBy != "" {
		queryString = fmt.Sprintf("%s ORDER BY %s", queryString, c.orderBy)
	}

	if c.limit > 0 {
		queryString = fmt.Sprintf("%s LIMIT %d", queryString, c.limit)
	}

	if c.page > 0 {
		var offset = 0
		if c.page > 1 {
			offset = (c.page - 1) * c.limit
		}

		queryString = fmt.Sprintf("%s OFFSET %d", queryString, offset)
	}

	if c.wrapJSON {
		queryString = fmt.Sprintf(`
WITH alias AS (
%s
)
SELECT to_jsonb(row_to_json(alias)) AS alias
FROM alias
		`, queryString)
	}

	return
}

func (c *config) GetPagingFunc(f ...Handler) Handler {
	if c.qf != nil {
		return c.qf.GetPaginationFunc()
	}

	if len(f) > 0 {
		return f[0]
	}

	return nil
}

// PagingFunc paging
func (c *config) PagingFunc(f ...Handler) *Pagination {
	if c.page < 1 {
		c.page = 1
	}
	var fn = c.GetPagingFunc(f...)
	if fn == nil {
		panic(fmt.Errorf("fn is not implement"))
	}

	var offset = (c.page - 1) * c.limit
	var done = make(chan bool, 1)
	var pagination Pagination
	var count int

	sqlString, countSQLString := c.build()

	var values = c.mergeValues()
	countSQLString = fmt.Sprintf(`
SELECT COUNT(1) 
FROM (
%s
) t
	`, countSQLString)
	var countSQL = c.db.GormDB().Raw(countSQLString, values...)
	go c.count(countSQL, done, &count)

	result, _ := fn(c.db, c.db.GormDB().Raw(sqlString, values...))
	<-done
	close(done)

	pagination.TotalRecord = count
	pagination.Records = result
	pagination.Page = c.page
	pagination.Offset = offset

	if c.limit > 0 {
		pagination.PerPage = c.limit
		pagination.TotalPage = int(math.Ceil(float64(count) / float64(c.limit)))
	} else {
		pagination.TotalPage = 1
		pagination.PerPage = count
	}

	if c.page > 1 {
		pagination.PrevPage = c.page - 1
	} else {
		pagination.PrevPage = c.page
	}

	if c.page == pagination.TotalPage {
		pagination.NextPage = c.page
	} else {
		pagination.NextPage = c.page + 1
	}

	pagination.HasNext = pagination.TotalPage > pagination.Page
	pagination.HasPrev = pagination.Page > 1

	if !pagination.HasNext {
		pagination.NextPage = pagination.Page
	}

	return &pagination
}

func (c *config) FindFunc(dest interface{}, f ...Handler) error {
	sqlString, _ := c.build()

	var rOut = reflect.ValueOf(dest)
	if rOut.Kind() != reflect.Ptr {
		return fmt.Errorf("must be a pointer of %T", dest)
	}

	var fn = c.GetPagingFunc(f...)
	if fn == nil {
		panic(fmt.Errorf("fn is not implement"))
	}

	var values = c.mergeValues()
	result, err := fn(c.db, c.db.GormDB().Raw(sqlString, values...))
	if err != nil {
		return err
	}

	return c.copyResult(rOut, result)
}

func (c *config) FirstFunc(dest interface{}, f ...Handler) error {
	c.limit = 1
	sqlString, _ := c.build()

	var rOut = reflect.ValueOf(dest)
	if rOut.Kind() != reflect.Ptr {
		return fmt.Errorf("must be a pointer of %T", dest)
	}

	var fn = c.GetPagingFunc(f...)
	if fn == nil {
		panic(fmt.Errorf("fn is not implement"))
	}

	var values = c.mergeValues()
	result, err := fn(c.db, c.db.GormDB().Raw(sqlString, values...))
	if err != nil {
		return err
	}
	return c.copyResult(rOut, result)
}

func (c *config) Scan(dest interface{}) error {
	sqlString, _ := c.build()

	var values = c.mergeValues()
	var result = c.db.GormDB().Raw(sqlString, values...).Scan(dest)
	if result.Error != nil {
		if result.RowsAffected == 0 {
			return sql.ErrNoRows
		}
	}

	return result.Error
}

func (c *config) Find(dest interface{}) error {
	sqlString, _ := c.build()

	var values = c.mergeValues()
	var result = c.db.GormDB().Raw(sqlString, values...).Find(dest)
	if result.Error != nil {
		if result.RowsAffected == 0 {
			return sql.ErrNoRows
		}
	}

	return result.Error
}

func (c *config) ExplainSQL() string {
	sqlString, _ := c.build()

	var values = c.mergeValues()
	var stmt = c.db.GormDB().Session(&gorm.Session{DryRun: true}).Raw(sqlString, values...).Statement
	return stmt.Explain(stmt.SQL.String(), stmt.Vars...)

}

func (c *config) ScanRow(dest interface{}) error {
	sqlString, _ := c.build()

	var values = c.mergeValues()
	var err = c.db.GormDB().Raw(sqlString, values).Row().Scan(dest)
	if err != nil {
		return err
	}

	return nil
}
func (c *config) count(db *gorm.DB, done chan bool, count *int) {
	if db != nil {
		db.Row().Scan(count)
	}
	done <- true
}

func (c *config) mergeValues() []interface{} {
	var values = []interface{}{}
	values = append(values, c.whereValues...)
	if len(c.namedWhereValues) > 0 {
		values = append(values, c.namedWhereValues)
	}
	return values
}

func (c *config) copyResult(rOut reflect.Value, result interface{}) error {
	var rResult = reflect.ValueOf(result)

	if rResult.Kind() != reflect.Ptr {
		rResult = toPtr(rResult)

	}

	if rResult.Type() != rOut.Type() {
		switch rResult.Kind() {
		case reflect.Array, reflect.Slice:
			if rResult.Len() > 0 {
				var elem = rResult.Index(0).Elem()
				rOut.Elem().Set(elem)
				return nil
			} else {
				return sql.ErrNoRows
			}
		case reflect.Ptr:
			switch rResult.Elem().Kind() {
			case reflect.Array, reflect.Slice:
				if rResult.Elem().Len() > 0 {
					var elem = rResult.Elem().Index(0).Elem()
					rOut.Elem().Set(elem)
					return nil
				} else {
					return sql.ErrNoRows
				}
			}
		}

		return fmt.Errorf("%v is not %v", rResult.Type(), rOut.Type())
	}

	rOut.Elem().Set(rResult.Elem())

	return nil
}

func toPtr(v reflect.Value) reflect.Value {
	pt := reflect.PtrTo(v.Type()) // create a *T type.
	pv := reflect.New(pt.Elem())  // create a reflect.Value of type *T.
	pv.Elem().Set(v)              // sets pv to point to underlying value of v.
	return pv
}
