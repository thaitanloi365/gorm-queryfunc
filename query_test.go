package queryfunc

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type GDB struct {
	*gorm.DB
}

func (gdb *GDB) GormDB() *gorm.DB {
	return gdb.DB
}

type Model struct {
	ID        uint           `gorm:"primarykey" json:"id,omitempty"`
	CreatedAt time.Time      `json:"created_at,omitempty"`
	UpdatedAt time.Time      `json:"updated_at,omitempty"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at,omitempty"`
}

type User struct {
	Model

	Name  string `json:"name"`
	Email string `json:"email"`

	CompanyID string   `json:"company_id"`
	Company   *Company `json:"company"`

	Devices []*Device `json:"devices"`
}

type Company struct {
	Model

	Name string `json:"name"`
}

type Device struct {
	Model

	UserID   uint   `json:"user_id"`
	Token    string `json:"token"`
	Platform string `json:"platform"`
}

/// Test start
var db *gorm.DB
var schemes = []interface{}{
	&User{},
	&Device{},
	&Company{},
}

func init() {
	var err error
	db, err = gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{
		PrepareStmt:                              true,
		DisableForeignKeyConstraintWhenMigrating: true,
		SkipDefaultTransaction:                   true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic(err)
	}

	db.AutoMigrate(schemes...)

	var users []*User
	for i := 0; i < 25; i++ {
		var user = &User{
			Model: Model{
				ID: uint(i),
			},
			Name:  fmt.Sprintf("User %d", i),
			Email: fmt.Sprintf("user_%d@gmail.com", i),
			Devices: []*Device{
				{
					Model: Model{
						ID: uint(i),
					},
					Token:    fmt.Sprintf("token_%d_1", i),
					Platform: "iOS",
				},
				{
					Model: Model{
						ID: uint(i + 1),
					},
					Token:    fmt.Sprintf("token_%d_2", i),
					Platform: "Android",
				},
			},
			Company: &Company{
				Model: Model{
					ID: 1,
				},
				Name: "Test Company 1",
			},
		}

		if i > 10 {
			user.Company = &Company{
				Model: Model{
					ID: 2,
				},
				Name: "Test Company 2",
			}
		}
		users = append(users, user)
	}

	err = db.Clauses(clause.OnConflict{DoNothing: true}).Create(&users).Error
	if err != nil {
		panic(err)
	}
}

func printJSON(i interface{}) {
	data, _ := json.MarshalIndent(&i, "", "   ")

	fmt.Println(string(data))
}

/// Builders

func NewDeviceQueryBuilder() *Builder {
	const rawSQL = `
	SELECT d.*
	FROM devices d
	`

	const countSQL = `
	SELECT 1
	FROM devices d
	`
	return NewBuilder(rawSQL, countSQL).
		WithPaginationFunc(func(db DB, rawSQL *gorm.DB) (interface{}, error) {
			var records []*Device
			var err = rawSQL.Find(&records).Error
			return &records, err
		})
}

func NewUserQueryBuilder() *Builder {
	const rawSQL = `
	SELECT u.*,
	c.id AS c__id,
	c.created_at AS c__created_at,
	c.updated_at AS c__updated_at,
	c.name AS c__name
	FROM users u
	LEFT JOIN companies c ON c.id = u.company_id
	`

	const countSQL = `
	SELECT 1
	FROM users u
	LEFT JOIN companies c ON c.id = u.company_id
	`
	return NewBuilder(rawSQL, countSQL).
		WithPaginationFunc(func(db DB, rawSQL *gorm.DB) (interface{}, error) {
			type userCopy struct {
				*User

				Company *Company `gorm:"embedded;embeddedPrefix:c__"`
			}

			var records = make([]*User, rawSQL.RowsAffected)
			rows, err := rawSQL.Rows()
			if err != nil {
				return nil, err

			}
			defer rows.Close()

			var userID []uint
			for rows.Next() {
				var copy userCopy
				err = db.GormDB().ScanRows(rows, &copy)
				if err != nil {
					continue
				}

				copy.User.Company = copy.Company

				records = append(records, copy.User)

				userID = append(userID, copy.User.ID)
			}

			if len(userID) > 0 {
				var devices []*Device
				var err = New(db, NewDeviceQueryBuilder()).FindFunc(&devices)
				if err == nil {
					for _, record := range records {
						for _, device := range devices {
							if device.UserID == record.ID {
								record.Devices = append(record.Devices, device)
							}
						}
					}
				}
			}
			return &records, err
		})
}

func TestPagination1(t *testing.T) {
	var gdb = &GDB{
		DB: db,
	}

	var result = New(gdb, NewUserQueryBuilder()).
		Page(1).
		Limit(5).
		WhereFunc(func(builder *config) {
			builder.Where("c.name = ?", "Test Company 1")
		}).
		PagingFunc()

	printJSON(result)
}

func TestPagination2(t *testing.T) {
	var gdb = &GDB{
		DB: db,
	}

	var result = New(gdb, NewDeviceQueryBuilder()).
		Page(1).
		Limit(5).
		WhereFunc(func(builder *config) {
			builder.Where("d.user_id = ?", 1)
		}).
		PagingFunc()

	printJSON(result)
}

func TestFirstFunc(t *testing.T) {
	var gdb = &GDB{
		DB: db,
	}

	var user User
	var err = New(gdb, NewUserQueryBuilder()).
		Page(1).
		Limit(5).
		WhereFunc(func(builder *config) {
			builder.Where("u.id = ?", 1)
		}).
		FirstFunc(&user)
	if err != nil {
		panic(err)
	}

	printJSON(user)
}

func TestFindFunc(t *testing.T) {
	var gdb = &GDB{
		DB: db,
	}

	var users []*User
	var err = New(gdb, NewUserQueryBuilder()).
		Page(1).
		Limit(5).
		WhereFunc(func(builder *config) {
			builder.Where("u.id IN ?", []uint{1, 2})
		}).
		FindFunc(&users)
	if err != nil {
		panic(err)
	}

	printJSON(users)
}
