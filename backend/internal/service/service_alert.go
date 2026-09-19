package service

import (
	"context"
	"fmt"
	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"log/slog"
	"time"
)

type AlertService struct {
	recipients    *repository.RecipientRepository
	subscriptions *repository.SubscriptionRepository
	alerts        *repository.AlertRecordRepository
	sms           *SMSService
	timeout       time.Duration
	now           func() time.Time
	logger        *slog.Logger
}

func NewAlertService(recipients *repository.RecipientRepository, subscriptions *repository.SubscriptionRepository, alerts *repository.AlertRecordRepository, sms *SMSService, timeout time.Duration, logger *slog.Logger) *AlertService {
	return &AlertService{recipients: recipients, subscriptions: subscriptions, alerts: alerts, sms: sms, timeout: timeout, now: time.Now, logger: logger}
}

// SendOverdueAlerts 扫描超时未确认的关怀对象，按确认周期分级告警：
// 超过一倍时限向所有启用家属发送普通告警，超过两倍时限追加升级告警。
// 每个确认周期内每级对同一订阅只成功发送一次；确认后周期结束，再次超时从普通级重来。
func (s *AlertService) SendOverdueAlerts(ctx context.Context) (int, error) {
	now := s.now().UTC()
	items, err := s.recipients.Overdue(ctx, now.Add(-s.timeout))
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, recipient := range items {
		ref := alertCycleRef(recipient)
		subs, err := s.subscriptions.ListActiveByRecipient(ctx, recipient.ID)
		if err != nil {
			return sent, fmt.Errorf("list subscriptions for recipient %d: %w", recipient.ID, err)
		}
		for _, level := range dueAlertLevels(now.Sub(ref), s.timeout) {
			for _, sub := range subs {
				rid, sid := recipient.ID, sub.ID
				claimed, err := s.alerts.Claim(ctx, &model.AlertRecord{CareRecipientID: rid, FamilySubscriptionID: sid, CycleRef: ref, Level: level, SentAt: now})
				if err != nil {
					return sent, fmt.Errorf("claim %s alert for subscription %d: %w", level, sid, err)
				}
				if !claimed {
					continue
				}
				if err := s.sms.Send(ctx, SMSMessage{CareRecipientID: &rid, FamilySubscriptionID: &sid, RecipientPhone: sub.FamilyPhone, Content: alertContent(level, recipient.Name), Kind: constants.SMSKindAlert}); err != nil {
					if releaseErr := s.alerts.Release(ctx, rid, sid, ref, level); releaseErr != nil {
						s.logger.Error("release alert claim failed", "recipient_id", rid, "subscription_id", sid, "level", level, "error", releaseErr)
					}
					return sent, fmt.Errorf("send %s alert to subscription %d: %w", level, sid, err)
				}
				sent++
			}
		}
	}
	s.logger.Info("overdue alerts dispatched", "count", sent)
	return sent, nil
}

// alertCycleRef 返回当前确认周期的起点：最近一次确认时间；从未确认过则为关怀开始时间。
// 确认会更新 LastConfirmedAt，周期起点随之变化，旧周期记录自然失效。
func alertCycleRef(recipient model.CareRecipient) time.Time {
	if recipient.LastConfirmedAt != nil {
		return recipient.LastConfirmedAt.UTC()
	}
	return recipient.CareStartAt.UTC()
}

// dueAlertLevels 按超时时长返回待发送的告警级别：一倍时限普通级，两倍时限追加升级级。
func dueAlertLevels(elapsed, timeout time.Duration) []string {
	switch {
	case elapsed >= 2*timeout:
		return []string{constants.AlertLevelNormal, constants.AlertLevelEscalated}
	case elapsed >= timeout:
		return []string{constants.AlertLevelNormal}
	default:
		return nil
	}
}

func alertContent(level, name string) string {
	if level == constants.AlertLevelEscalated {
		return fmt.Sprintf("紧急关怀告警：%s 已超过两倍确认时限仍未回复，请立即联系确认情况。", name)
	}
	return fmt.Sprintf("关怀提醒：%s 已超过确认时限未回复，请尽快联系确认。", name)
}
