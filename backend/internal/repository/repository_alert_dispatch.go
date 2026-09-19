package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"gorm.io/gorm"
)

// ErrDispatchClaimed 表示该周期、该家属、该级别的告警已被其它扫描占位或成功发送。
var ErrDispatchClaimed = errors.New("alert dispatch already claimed")

type AlertDispatchRepository struct{ db *gorm.DB }

func NewAlertDispatchRepository(db *gorm.DB) *AlertDispatchRepository {
	return &AlertDispatchRepository{db: db}
}

// Claim 在唯一索引上占位 pending。若同一周期/家属/级别已存在：
//   - success 行：本级别本周期已发送，返回 ErrDispatchClaimed；
//   - pending 且未过期：其它扫描正在处理，返回 ErrDispatchClaimed；
//   - pending 但已过期（上次发送进程崩溃）：抢占后重试。
//
// 依赖唯一索引保证并发扫描只有一个 Claim 成功。
func (r *AlertDispatchRepository) Claim(ctx context.Context, recipientID uint, cycleKey string, subscriptionID uint, level string, staleAfter, now time.Time) (uint, error) {
	db := r.db.WithContext(ctx)
	var existing model.AlertDispatch
	lookupErr := db.Where("care_recipient_id = ? AND cycle_key = ? AND family_subscription_id = ? AND alert_level = ?",
		recipientID, cycleKey, subscriptionID, level).First(&existing).Error
	if lookupErr == nil {
		if existing.Status == constants.AlertDispatchSuccess || existing.AttemptAt.After(staleAfter) {
			return 0, ErrDispatchClaimed
		}
		result := db.Model(&model.AlertDispatch{}).Where("id = ? AND status = ? AND attempt_at <= ?",
			existing.ID, constants.AlertDispatchPending, staleAfter).
			Update("attempt_at", now)
		if result.Error != nil {
			return 0, fmt.Errorf("reclaim stale alert dispatch: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return 0, ErrDispatchClaimed
		}
		return existing.ID, nil
	}
	if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("lookup alert dispatch: %w", lookupErr)
	}
	item := &model.AlertDispatch{
		CareRecipientID:      recipientID,
		CycleKey:             cycleKey,
		FamilySubscriptionID: subscriptionID,
		AlertLevel:           level,
		Status:               constants.AlertDispatchPending,
		AttemptAt:            now,
	}
	if err := db.Create(item).Error; err != nil {
		if isDuplicateKey(err) {
			return 0, ErrDispatchClaimed
		}
		return 0, fmt.Errorf("create alert dispatch: %w", err)
	}
	return item.ID, nil
}

// MarkSent 在短信确认发送成功后调用；只有成功才把占位转为已发。
func (r *AlertDispatchRepository) MarkSent(ctx context.Context, id uint, smsLogID *uint, now time.Time) error {
	result := r.db.WithContext(ctx).Model(&model.AlertDispatch{}).
		Where("id = ? AND status = ?", id, constants.AlertDispatchPending).
		Updates(map[string]any{"status": constants.AlertDispatchSuccess, "sms_log_id": smsLogID, "sent_at": now, "attempt_at": now})
	if result.Error != nil {
		return fmt.Errorf("mark alert dispatch sent: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// Release 发送失败时释放占位，使后续扫描可以重试，失败不记为已发。
func (r *AlertDispatchRepository) Release(ctx context.Context, id uint) error {
	if err := r.db.WithContext(ctx).Where("id = ? AND status = ?", id, constants.AlertDispatchPending).
		Delete(&model.AlertDispatch{}).Error; err != nil {
		return fmt.Errorf("release alert dispatch: %w", err)
	}
	return nil
}

// SuccessLevels 返回当前周期内已成功发送过的告警级别集合。
func (r *AlertDispatchRepository) SuccessLevels(ctx context.Context, recipientID uint, cycleKey string) (map[string]bool, error) {
	var levels []string
	if err := r.db.WithContext(ctx).Model(&model.AlertDispatch{}).
		Where("care_recipient_id = ? AND cycle_key = ? AND status = ?",
			recipientID, cycleKey, constants.AlertDispatchSuccess).
		Distinct().Pluck("alert_level", &levels).Error; err != nil {
		return nil, fmt.Errorf("list success alert levels: %w", err)
	}
	result := make(map[string]bool, len(levels))
	for _, level := range levels {
		result[level] = true
	}
	return result, nil
}

func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "duplicate entry") || strings.Contains(message, "unique constraint failed")
}
