package service

import (
	"context"
	"errors"
	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingSMSProvider struct {
	mu       sync.Mutex
	messages []SMSMessage
	failures int
}

func (p *recordingSMSProvider) Send(_ context.Context, msg SMSMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("sms provider unavailable")
	}
	p.messages = append(p.messages, msg)
	return nil
}

func (p *recordingSMSProvider) snapshot() []SMSMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]SMSMessage(nil), p.messages...)
}

func newAlertTestService(t *testing.T) (*AlertService, *recordingSMSProvider, *repository.RecipientRepository, *repository.SubscriptionRepository) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	recipientRepo := repository.NewRecipientRepository(db)
	subscriptionRepo := repository.NewSubscriptionRepository(db)
	provider := &recordingSMSProvider{}
	svc := NewAlertService(recipientRepo, subscriptionRepo, repository.NewAlertRecordRepository(db), NewSMSService(provider), time.Hour, logger)
	return svc, provider, recipientRepo, subscriptionRepo
}

func TestAlertServiceGradedDedupClosedLoop(t *testing.T) {
	svc, provider, recipientRepo, subscriptionRepo := newAlertTestService(t)
	ctx := context.Background()
	base := time.Now().UTC()
	current := base
	svc.now = func() time.Time { return current }

	confirmedAt := base.Add(-30 * time.Minute)
	recipient := model.CareRecipient{Name: "张奶奶", Phone: "100001", CareFrequency: "daily", CareStartAt: base.Add(-72 * time.Hour), Status: constants.RecipientStatusActive, LastConfirmedAt: &confirmedAt}
	if err := recipientRepo.Create(ctx, &recipient); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []model.FamilySubscription{
		{CareRecipientID: recipient.ID, FamilyName: "女儿", FamilyPhone: "200001", Active: true},
		{CareRecipientID: recipient.ID, FamilyName: "儿子", FamilyPhone: "200002", Active: false},
	} {
		item := sub
		if err := subscriptionRepo.Create(ctx, &item); err != nil {
			t.Fatal(err)
		}
	}
	paused := model.CareRecipient{Name: "李爷爷", Phone: "100003", CareFrequency: "daily", CareStartAt: base.Add(-72 * time.Hour), Status: constants.RecipientStatusPaused}
	if err := recipientRepo.Create(ctx, &paused); err != nil {
		t.Fatal(err)
	}
	if err := subscriptionRepo.Create(ctx, &model.FamilySubscription{CareRecipientID: paused.ID, FamilyName: "孙子", FamilyPhone: "200003", Active: true}); err != nil {
		t.Fatal(err)
	}

	// 首次超时：普通级告警一次，仅发给启用订阅，暂停对象不发送。
	current = confirmedAt.Add(61 * time.Minute)
	sent, err := svc.SendOverdueAlerts(ctx)
	if err != nil || sent != 1 {
		t.Fatalf("first scan sent=%d err=%v, want 1", sent, err)
	}
	msgs := provider.snapshot()
	if len(msgs) != 1 || msgs[0].RecipientPhone != "200001" {
		t.Fatalf("messages=%+v, want one message to 200001", msgs)
	}
	if !strings.Contains(msgs[0].Content, "关怀提醒") || strings.Contains(msgs[0].Content, "紧急") {
		t.Fatalf("first alert content=%q, want normal level", msgs[0].Content)
	}

	// 重复扫描：同级同周期去重，不再发送。
	if sent, err = svc.SendOverdueAlerts(ctx); err != nil || sent != 0 {
		t.Fatalf("duplicate scan sent=%d err=%v, want 0", sent, err)
	}

	// 超过两倍时限且并发扫描：升级告警只生效一次。
	current = confirmedAt.Add(2*time.Hour + time.Minute)
	var wg sync.WaitGroup
	var total atomic.Int32
	errs := make([]error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n, scanErr := svc.SendOverdueAlerts(ctx)
			total.Add(int32(n))
			errs[i] = scanErr
		}(i)
	}
	wg.Wait()
	for _, scanErr := range errs {
		if scanErr != nil {
			t.Fatalf("concurrent scan err=%v", scanErr)
		}
	}
	if total.Load() != 1 {
		t.Fatalf("concurrent scans sent=%d, want exactly 1", total.Load())
	}
	msgs = provider.snapshot()
	if len(msgs) != 2 || !strings.Contains(msgs[1].Content, "紧急") {
		t.Fatalf("messages=%+v, want escalated alert appended", msgs)
	}
	if sent, err = svc.SendOverdueAlerts(ctx); err != nil || sent != 0 {
		t.Fatalf("post-escalation scan sent=%d err=%v, want 0", sent, err)
	}

	// 确认后本周期立即结束：不再告警。
	newConfirm := current
	recipient.LastConfirmedAt = &newConfirm
	if err := recipientRepo.Update(ctx, &recipient); err != nil {
		t.Fatal(err)
	}
	if sent, err = svc.SendOverdueAlerts(ctx); err != nil || sent != 0 {
		t.Fatalf("after confirm sent=%d err=%v, want 0", sent, err)
	}

	// 新周期再次超时：从普通级重来。
	current = newConfirm.Add(61 * time.Minute)
	if sent, err = svc.SendOverdueAlerts(ctx); err != nil || sent != 1 {
		t.Fatalf("new cycle sent=%d err=%v, want 1", sent, err)
	}
	msgs = provider.snapshot()
	if len(msgs) != 3 || !strings.Contains(msgs[2].Content, "关怀提醒") || strings.Contains(msgs[2].Content, "紧急") {
		t.Fatalf("new cycle messages=%+v, want normal alert", msgs)
	}

	// 发送失败不得记为已发：后续扫描必须重试直至成功。
	current = newConfirm.Add(2*time.Hour + time.Minute)
	provider.failures = 1
	if sent, err = svc.SendOverdueAlerts(ctx); err == nil || sent != 0 {
		t.Fatalf("failing scan sent=%d err=%v, want error and 0", sent, err)
	}
	if sent, err = svc.SendOverdueAlerts(ctx); err != nil || sent != 1 {
		t.Fatalf("retry scan sent=%d err=%v, want 1", sent, err)
	}
	if sent, err = svc.SendOverdueAlerts(ctx); err != nil || sent != 0 {
		t.Fatalf("post-retry scan sent=%d err=%v, want 0", sent, err)
	}
	msgs = provider.snapshot()
	if len(msgs) != 4 || !strings.Contains(msgs[3].Content, "紧急") {
		t.Fatalf("final messages=%+v, want retried escalated alert", msgs)
	}
	for _, msg := range msgs {
		if msg.RecipientPhone != "200001" {
			t.Fatalf("unexpected message to %q, paused recipient or inactive subscription must not receive alerts", msg.RecipientPhone)
		}
	}
}

func TestAlertRecordRepositoryClaim(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewAlertRecordRepository(db)
	ctx := context.Background()
	ref := time.Now().UTC().Truncate(time.Second)
	claim := func(recipientID, subID uint, cycleRef time.Time, level string) bool {
		claimed, err := repo.Claim(ctx, &model.AlertRecord{CareRecipientID: recipientID, FamilySubscriptionID: subID, CycleRef: cycleRef, Level: level, SentAt: cycleRef})
		if err != nil {
			t.Fatal(err)
		}
		return claimed
	}
	if !claim(1, 1, ref, constants.AlertLevelNormal) {
		t.Fatal("first claim must succeed")
	}
	if claim(1, 1, ref, constants.AlertLevelNormal) {
		t.Fatal("duplicate claim in same cycle and level must fail")
	}
	if !claim(1, 1, ref, constants.AlertLevelEscalated) {
		t.Fatal("different level in same cycle must be claimable")
	}
	if !claim(1, 1, ref.Add(time.Hour), constants.AlertLevelNormal) {
		t.Fatal("new cycle must be claimable from normal level")
	}
	if !claim(1, 2, ref, constants.AlertLevelNormal) {
		t.Fatal("different subscription must be claimable")
	}
	if err := repo.Release(ctx, 1, 1, ref, constants.AlertLevelEscalated); err != nil {
		t.Fatal(err)
	}
	if !claim(1, 1, ref, constants.AlertLevelEscalated) {
		t.Fatal("released claim must be retryable")
	}

	var wg sync.WaitGroup
	var wins atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := repo.Claim(ctx, &model.AlertRecord{CareRecipientID: 9, FamilySubscriptionID: 9, CycleRef: ref, Level: constants.AlertLevelNormal, SentAt: ref})
			if err != nil {
				t.Error(err)
				return
			}
			if claimed {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("concurrent claims won=%d, want exactly 1", wins.Load())
	}
}
