package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
)

// alertDispatchStaleTTL：进程在占位后崩溃时，pending 记录允许被后续扫描抢占重试的时长。
const alertDispatchStaleTTL = 2 * time.Minute

// neverConfirmedCycleKey 是对象从未确认过的确认周期标识。
const neverConfirmedCycleKey = "never-confirmed"

// ConfirmationCycleKey 依据最近确认时间派生确认周期标识。
// 对象一旦确认（LastConfirmedAt 变化）即进入新周期，告警从普通级重新开始。
func ConfirmationCycleKey(lastConfirmedAt *time.Time) string {
	if lastConfirmedAt == nil {
		return neverConfirmedCycleKey
	}
	return lastConfirmedAt.UTC().Format(time.RFC3339Nano)
}

// alertBasis 返回计算超时时限的基准时间：优先取最近确认时间，未确认过则取关怀开始时间。
func alertBasis(recipient model.CareRecipient) time.Time {
	if recipient.LastConfirmedAt != nil {
		return recipient.LastConfirmedAt.UTC()
	}
	return recipient.CareStartAt.UTC()
}

type AlertService struct {
	recipients    *repository.RecipientRepository
	subscriptions *repository.SubscriptionRepository
	dispatches    *repository.AlertDispatchRepository
	sms           *SMSService
	timeout       time.Duration
	logger        *slog.Logger
	scanning      atomic.Bool
}

func NewAlertService(recipients *repository.RecipientRepository, subscriptions *repository.SubscriptionRepository, dispatches *repository.AlertDispatchRepository, sms *SMSService, timeout time.Duration, logger *slog.Logger) *AlertService {
	return &AlertService{recipients: recipients, subscriptions: subscriptions, dispatches: dispatches, sms: sms, timeout: timeout, logger: logger}
}

// SendOverdueAlerts 执行一次超时告警扫描：
//   - 超过 1 倍时限未确认：向每个启用家属发一次普通告警；
//   - 超过 2 倍时限仍未确认：再各发一次升级告警；
//   - 每个确认周期内每级告警对每个家属最多成功发送一次；
//   - 发送失败不记为已发，后续扫描重试；重复或并发扫描只生效一次。
func (s *AlertService) SendOverdueAlerts(ctx context.Context) (int, error) {
	if !s.scanning.CompareAndSwap(false, true) {
		s.logger.Warn("overdue alert scan skipped: another scan is running")
		return 0, nil
	}
	defer s.scanning.Store(false)

	now := time.Now().UTC()
	normalCutoff := now.Add(-s.timeout)
	escalatedCutoff := now.Add(-2 * s.timeout)
	items, err := s.recipients.Overdue(ctx, normalCutoff)
	if err != nil {
		return 0, err
	}
	sent := 0
	var failures []error
	for _, recipient := range items {
		basis := alertBasis(recipient)
		normalDue := basis.Before(normalCutoff)
		escalatedDue := basis.Before(escalatedCutoff)
		if !normalDue {
			continue
		}
		count, err := s.dispatchForRecipient(ctx, recipient, normalDue, escalatedDue, now)
		sent += count
		if err != nil {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		s.logger.Error("overdue alerts dispatched with failures", "sent", sent, "error", err)
		return sent, err
	}
	s.logger.Info("overdue alerts dispatched", "count", sent)
	return sent, nil
}

// dispatchForRecipient 处理单个对象在当前扫描中的两级告警。
func (s *AlertService) dispatchForRecipient(ctx context.Context, recipient model.CareRecipient, normalDue, escalatedDue bool, now time.Time) (int, error) {
	subs, err := s.subscriptions.ListActiveByRecipient(ctx, recipient.ID)
	if err != nil {
		return 0, fmt.Errorf("list subscriptions for recipient %d: %w", recipient.ID, err)
	}
	if len(subs) == 0 {
		return 0, nil
	}
	cycleKey := ConfirmationCycleKey(recipient.LastConfirmedAt)
	levels := make([]string, 0, 2)
	if normalDue {
		levels = append(levels, constants.AlertLevelNormal)
	}
	if escalatedDue {
		levels = append(levels, constants.AlertLevelEscalated)
	}
	sent := 0
	var failures []error
	// 逐家属、逐级别占位：是否已发由 (周期, 家属, 级别) 唯一索引判定，
	// 因此周期中途新增的家属订阅也能补齐其尚未收到的告警级别。
	for _, sub := range subs {
		for _, level := range levels {
			ok, err := s.dispatchOne(ctx, recipient, sub, cycleKey, level, now)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			if ok {
				sent++
			}
		}
	}
	return sent, errors.Join(failures...)
}

// dispatchOne 占位并发送单条告警。返回 ok=false 表示该级别本周期已发送或已被并发扫描占用。
func (s *AlertService) dispatchOne(ctx context.Context, recipient model.CareRecipient, sub model.FamilySubscription, cycleKey, level string, now time.Time) (bool, error) {
	dispatchID, err := s.dispatches.Claim(ctx, recipient.ID, cycleKey, sub.ID, level, now.Add(-alertDispatchStaleTTL), now)
	if errors.Is(err, repository.ErrDispatchClaimed) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim %s alert for subscription %d: %w", level, sub.ID, err)
	}
	kind, content := constants.SMSKindAlert, fmt.Sprintf("关怀提醒：%s 已超过确认时限未回复，请尽快联系确认。", recipient.Name)
	if level == constants.AlertLevelEscalated {
		kind = constants.SMSKindAlertEscalated
		content = fmt.Sprintf("紧急告警：%s 已超过两倍确认时限仍未回复确认，请立即联系核实。", recipient.Name)
	}
	rid, sid := recipient.ID, sub.ID
	message := SMSMessage{CareRecipientID: &rid, FamilySubscriptionID: &sid, RecipientPhone: sub.FamilyPhone, Content: content, Kind: kind}
	if sendErr := s.sms.Send(ctx, message); sendErr != nil {
		s.logger.Warn("alert send failed, will retry in later scans", "recipient_id", rid, "subscription_id", sid, "level", level, "error", sendErr)
		if releaseErr := s.dispatches.Release(ctx, dispatchID); releaseErr != nil {
			s.logger.Error("release claimed alert dispatch failed", "dispatch_id", dispatchID, "error", releaseErr)
		}
		return false, fmt.Errorf("send %s alert to subscription %d: %w", level, sub.ID, sendErr)
	}
	sentAt := time.Now().UTC()
	if err := s.dispatches.MarkSent(ctx, dispatchID, nil, sentAt); err != nil {
		// 短信已送达但状态落库失败：保留占位（到期前不再重发），交由后续对账，避免重复打扰家属。
		s.logger.Error("mark alert dispatch sent failed", "dispatch_id", dispatchID, "error", err)
		return false, fmt.Errorf("mark %s alert sent for subscription %d: %w", level, sub.ID, err)
	}
	s.logger.Info("overdue alert dispatched", "recipient_id", rid, "subscription_id", sid, "level", level)
	return true, nil
}
