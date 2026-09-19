package service

import (
	"context"
	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"time"
)

type RecipientStatus struct {
	Recipient  model.CareRecipient `json:"recipient"`
	State      string              `json:"state"`
	Reason     string              `json:"reason"`
	AlertLevel string              `json:"alert_level"`
}

type StatusService struct {
	recipients *repository.RecipientRepository
	alerts     *repository.AlertRecordRepository
	timeout    time.Duration
}

func NewStatusService(recipients *repository.RecipientRepository, alerts *repository.AlertRecordRepository, timeout time.Duration) *StatusService {
	return &StatusService{recipients: recipients, alerts: alerts, timeout: timeout}
}
func (s *StatusService) List(ctx context.Context, page, pageSize int) ([]RecipientStatus, int64, error) {
	items, total, err := s.recipients.List(ctx, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]uint, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	records, err := s.alerts.ListByRecipientIDs(ctx, ids)
	if err != nil {
		return nil, 0, err
	}
	levels := highestAlertLevels(items, records)
	result := make([]RecipientStatus, 0, len(items))
	now := time.Now().UTC()
	cutoff := now.Add(-s.timeout)
	for _, item := range items {
		state, reason := "失联", "超过确认时限"
		switch {
		case item.Status == constants.RecipientStatusPaused:
			state, reason = "已暂停", "关怀已暂停"
		case item.LastConfirmedAt != nil && !item.LastConfirmedAt.Before(cutoff):
			state, reason = "平安", "近期已确认"
		case item.LastConfirmedAt == nil && item.CareStartAt.After(cutoff):
			state, reason = "待确认", "尚未确认"
		}
		result = append(result, RecipientStatus{Recipient: item, State: state, Reason: reason, AlertLevel: levels[item.ID]})
	}
	return result, total, nil
}

// highestAlertLevels 计算每个关怀对象当前确认周期内已成功送达的最高告警级别，
// 历史周期的记录不计入；无记录时返回 AlertLevelNone。
func highestAlertLevels(recipients []model.CareRecipient, records []model.AlertRecord) map[uint]string {
	refs := make(map[uint]time.Time, len(recipients))
	for _, item := range recipients {
		refs[item.ID] = alertCycleRef(item)
	}
	levels := make(map[uint]string, len(recipients))
	for _, record := range records {
		ref, ok := refs[record.CareRecipientID]
		if !ok || !record.CycleRef.Equal(ref) {
			continue
		}
		if alertLevelRank(record.Level) > alertLevelRank(levels[record.CareRecipientID]) {
			levels[record.CareRecipientID] = record.Level
		}
	}
	for _, item := range recipients {
		if _, ok := levels[item.ID]; !ok {
			levels[item.ID] = constants.AlertLevelNone
		}
	}
	return levels
}

func alertLevelRank(level string) int {
	switch level {
	case constants.AlertLevelEscalated:
		return 2
	case constants.AlertLevelNormal:
		return 1
	default:
		return 0
	}
}
