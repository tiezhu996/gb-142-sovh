package service

import (
	"context"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
)

type RecipientStatus struct {
	Recipient  model.CareRecipient `json:"recipient"`
	State      string              `json:"state"`
	Reason     string              `json:"reason"`
	AlertLevel string              `json:"alert_level"`
}
type StatusService struct {
	recipients *repository.RecipientRepository
	timeout    time.Duration
}

func NewStatusService(recipients *repository.RecipientRepository, timeout time.Duration) *StatusService {
	return &StatusService{recipients: recipients, timeout: timeout}
}
func (s *StatusService) List(ctx context.Context, page, pageSize int) ([]RecipientStatus, int64, error) {
	items, total, err := s.recipients.List(ctx, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	result := make([]RecipientStatus, 0, len(items))
	now := time.Now().UTC()
	cutoff := now.Add(-s.timeout)
	escalatedCutoff := now.Add(-2 * s.timeout)
	for _, item := range items {
		state, reason, alertLevel := "失联", "超过确认时限（普通告警）", constants.AlertLevelNormal
		switch {
		case item.Status == constants.RecipientStatusPaused:
			state, reason, alertLevel = "已暂停", "关怀已暂停", ""
		case item.LastConfirmedAt != nil && !item.LastConfirmedAt.Before(cutoff):
			state, reason, alertLevel = "平安", "近期已确认", ""
		case item.LastConfirmedAt == nil && item.CareStartAt.After(cutoff):
			state, reason, alertLevel = "待确认", "尚未确认", ""
		default:
			if alertBasis(item).Before(escalatedCutoff) {
				alertLevel = constants.AlertLevelEscalated
				reason = "超过两倍确认时限（升级告警）"
			}
		}
		result = append(result, RecipientStatus{Recipient: item, State: state, Reason: reason, AlertLevel: alertLevel})
	}
	return result, total, nil
}
