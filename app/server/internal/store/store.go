package store

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"kafkavista/server/internal/config"
	"kafkavista/server/internal/model"
)

func Open(cfg config.Config) (*gorm.DB, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.DatabasePath), 0755); err != nil {
		return nil, err
	}
	return gorm.Open(sqlite.Open(cfg.DatabasePath), &gorm.Config{})
}

func Init(db *gorm.DB, cfg config.Config) error {
	if err := db.AutoMigrate(&model.KafkaCluster{}, &model.KafkaPermission{}, &model.KafkaRolePermission{}, &model.KafkaAuditLog{}, &model.KafkaGroupPlaceholder{}, &model.KafkaGroupMetric{}, &model.KafkaClusterMetric{}, &model.KafkaMigrationJob{}, &model.AppUser{}, &model.AppRole{}, &model.SystemSetting{}); err != nil {
		return err
	}
	db.Where(model.AppRole{Name: "admin"}).FirstOrCreate(&model.AppRole{Name: "admin", Description: "系统管理员"})
	db.Where(model.AppRole{Name: "user"}).FirstOrCreate(&model.AppRole{Name: "user", Description: "普通用户"})
	nowUser := model.AppUser{Username: cfg.DefaultUser, DisplayName: cfg.DefaultUser, Role: "admin", Source: "local", IsActive: true}
	db.Where(model.AppUser{Username: cfg.DefaultUser}).FirstOrCreate(&nowUser)
	var dbUser model.AppUser
	if db.Where(model.AppUser{Username: cfg.DefaultUser}).First(&dbUser).Error == nil && dbUser.PasswordHash == "" {
		if hash, err := HashPassword(cfg.DefaultPassword); err == nil {
			db.Model(&dbUser).Update("password_hash", hash)
		}
	}
	var count int64
	db.Model(&model.KafkaCluster{}).Where("name = ?", cfg.DefaultClusterName).Count(&count)
	if count > 0 {
		return nil
	}
	cluster := model.KafkaCluster{
		Name:             cfg.DefaultClusterName,
		ClusterType:      "cluster",
		BootstrapServers: cfg.DefaultBootstrapServers,
		SecurityProtocol: "PLAINTEXT",
		Description:      "默认 Kafka 集群",
		IsActive:         true,
	}
	if err := db.Create(&cluster).Error; err != nil {
		return err
	}
	for _, action := range model.AllActions {
		db.Create(&model.KafkaPermission{ClusterID: cluster.ID, Username: cfg.DefaultUser, Action: action, CreatedBy: "system"})
	}
	return nil
}

func HashPassword(password string) (string, error) {
	if password == "" {
		return "", errors.New("password required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func VerifyPassword(stored, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(stored), []byte(password)) == nil
}
