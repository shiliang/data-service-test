package strategies

import (
	"context"
	"data-integrate-test/config"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type VastbaseStrategy struct {
	config *config.DatabaseConfig
	db     *sql.DB
}

func NewVastbaseStrategy(config *config.DatabaseConfig) *VastbaseStrategy {
	return &VastbaseStrategy{config: config}
}

func (v *VastbaseStrategy) Connect(ctx context.Context) error {
	// VastBase使用PostgreSQL协议
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		v.config.Host, v.config.Port, v.config.User, v.config.Password, v.config.Database)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	if err := db.PingContext(ctx); err != nil {
		return err
	}

	v.db = db
	return nil
}

func (v *VastbaseStrategy) GetDB() *sql.DB {
	return v.db
}

func (v *VastbaseStrategy) GetDBType() string {
	return "vastbase"
}

func (v *VastbaseStrategy) GetConnectionInfo() *config.DatabaseConfig {
	return v.config
}

func (v *VastbaseStrategy) Cleanup(ctx context.Context, tableName string) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName)
	_, err := v.db.ExecContext(ctx, query)
	return err
}

func (v *VastbaseStrategy) GetRowCount(ctx context.Context, tableName string) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)
	var count int64
	err := v.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

func (v *VastbaseStrategy) TableExists(ctx context.Context, tableName string) (bool, error) {
	query := `
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_schema = current_schema()
		AND table_name = $1
	`
	var count int
	err := v.db.QueryRowContext(ctx, query, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
