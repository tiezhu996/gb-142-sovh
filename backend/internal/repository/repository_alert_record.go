package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/blueship581/gbcarenotify/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type AlertRecordRepository struct{ db *gorm.DB }

func NewAlertRecordRepository(db *gorm.DB) *AlertRecordRepository {
	return &AlertRecordRepository{db: db}
}

// Claim 尝试为一次告警写入认领记录。唯一索引保证同一确认周期内同一订阅同一级别
// 只成功一次：返回 false 表示已发送过或已被并发扫描认领，调用方不得重复发送。
func (r *AlertRecordRepository) Claim(ctx context.Context, item *model.AlertRecord) (bool, error) {
	result := r.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(item)
	if result.Error != nil {
		return false, fmt.Errorf("claim alert record: %w", result.Error)
	}
	return result.RowsAffected > 0, nil
}

// Release 删除发送失败留下的认领记录，使后续扫描能够重试同一告警。
func (r *AlertRecordRepository) Release(ctx context.Context, recipientID, subscriptionID uint, cycleRef time.Time, level string) error {
	result := r.db.WithContext(ctx).Where("care_recipient_id = ? AND family_subscription_id = ? AND cycle_ref = ? AND level = ?", recipientID, subscriptionID, cycleRef, level).Delete(&model.AlertRecord{})
	if result.Error != nil {
		return fmt.Errorf("release alert record: %w", result.Error)
	}
	return nil
}

// ListByRecipientIDs 返回指定关怀对象的全部告警记录，由调用方按确认周期过滤。
func (r *AlertRecordRepository) ListByRecipientIDs(ctx context.Context, recipientIDs []uint) ([]model.AlertRecord, error) {
	if len(recipientIDs) == 0 {
		return nil, nil
	}
	var items []model.AlertRecord
	if err := r.db.WithContext(ctx).Where("care_recipient_id IN ?", recipientIDs).Find(&items).Error; err != nil {
		return nil, fmt.Errorf("list alert records: %w", err)
	}
	return items, nil
}
