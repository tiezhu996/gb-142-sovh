package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/dto"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type recordingProvider struct {
	mu       sync.Mutex
	messages []SMSMessage
	failNext int32
}

func (p *recordingProvider) Send(_ context.Context, message SMSMessage) error {
	if atomic.AddInt32(&p.failNext, -1) >= 0 {
		return errors.New("provider unavailable")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	return nil
}

func (p *recordingProvider) snapshot() []SMSMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SMSMessage, len(p.messages))
	copy(out, p.messages)
	return out
}

type alertFixture struct {
	db           *gorm.DB
	recipients   *repository.RecipientRepository
	subs         *repository.SubscriptionRepository
	dispatches   *repository.AlertDispatchRepository
	provider     *recordingProvider
	alert        *AlertService
	recipientSvc *RecipientService
}

var testDBCounter int64

// openTestDB 使用共享缓存的内存 SQLite，并限制单连接：
// 既让并发 goroutine 看到同一份 schema，又串行化写入，模拟真实库的事务竞争点。
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:alert_test_%d?mode=memory&cache=shared", atomic.AddInt64(&testDBCounter, 1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
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
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func newAlertFixture(t *testing.T, timeout time.Duration) *alertFixture {
	t.Helper()
	db := openTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	recipientRepo := repository.NewRecipientRepository(db)
	subRepo := repository.NewSubscriptionRepository(db)
	dispatchRepo := repository.NewAlertDispatchRepository(db)
	provider := &recordingProvider{}
	svc := NewAlertService(recipientRepo, subRepo, dispatchRepo, NewSMSService(provider), timeout, logger)
	recipientSvc := NewRecipientService(recipientRepo, repository.NewReplyConfirmRepository(db), logger)
	return &alertFixture{db: db, recipients: recipientRepo, subs: subRepo, dispatches: dispatchRepo, provider: provider, alert: svc, recipientSvc: recipientSvc}
}

func (f *alertFixture) seedRecipient(t *testing.T, name, phone string, careStart time.Time, status string) model.CareRecipient {
	t.Helper()
	item := model.CareRecipient{Name: name, Phone: phone, CareFrequency: constants.FrequencyDaily, CareStartAt: careStart, Status: status}
	if err := f.recipients.Create(context.Background(), &item); err != nil {
		t.Fatal(err)
	}
	return item
}

func (f *alertFixture) seedSubscription(t *testing.T, recipientID uint, phone string, active bool) model.FamilySubscription {
	t.Helper()
	item := model.FamilySubscription{CareRecipientID: recipientID, FamilyName: "家属-" + phone, FamilyPhone: phone, Active: &active}
	if err := f.subs.Create(context.Background(), &item); err != nil {
		t.Fatal(err)
	}
	return item
}

func kindCount(messages []SMSMessage) map[string]int {
	counts := map[string]int{}
	for _, m := range messages {
		counts[m.Kind]++
	}
	return counts
}

// 首次超时：每个启用家属收到一次普通告警；重复扫描不重发；超过两倍时限后补发一次升级告警。
func TestAlertServiceNormalDedupThenEscalate(t *testing.T) {
	now := time.Now().UTC()
	f := newAlertFixture(t, time.Hour)
	recipient := f.seedRecipient(t, "王阿姨", "13800000001", now.Add(-90*time.Minute), constants.RecipientStatusActive)
	f.seedSubscription(t, recipient.ID, "13900000001", true)
	f.seedSubscription(t, recipient.ID, "13900000002", true)
	ctx := context.Background()

	// 超时 1 倍但未到 2 倍：只发普通告警，每个启用家属一次。
	sent, err := f.alert.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 2 {
		t.Fatalf("first scan sent=%d, want 2", sent)
	}
	if counts := kindCount(f.provider.snapshot()); counts[constants.SMSKindAlert] != 2 || counts[constants.SMSKindAlertEscalated] != 0 {
		t.Fatalf("first scan kinds=%v, want 2 normal", counts)
	}

	// 再次扫描：同一周期内普通级已成功，不得重复发送。
	sent, err = f.alert.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 0 || len(f.provider.snapshot()) != 2 {
		t.Fatalf("repeat scan sent=%d total=%d, want no duplicates", sent, len(f.provider.snapshot()))
	}

	// 模拟时间推进到超过两倍时限（缩短 timeout）：每个家属再各收一次升级告警。
	f.alert.timeout = 40 * time.Minute
	sent, err = f.alert.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 2 {
		t.Fatalf("escalation scan sent=%d, want 2", sent)
	}
	counts := kindCount(f.provider.snapshot())
	if counts[constants.SMSKindAlert] != 2 || counts[constants.SMSKindAlertEscalated] != 2 {
		t.Fatalf("kinds after escalation=%v", counts)
	}

	// 升级级也只发一次。
	if sent, _ = f.alert.SendOverdueAlerts(ctx); sent != 0 {
		t.Fatalf("repeat escalation scan sent=%d, want 0", sent)
	}
}

// 确认后周期结束、不再告警；之后再次超时从普通级重新开始。
func TestAlertServiceConfirmClosesCycleAndRestartsAtNormal(t *testing.T) {
	now := time.Now().UTC()
	f := newAlertFixture(t, time.Hour)
	recipient := f.seedRecipient(t, "李伯伯", "13800000002", now.Add(-90*time.Minute), constants.RecipientStatusActive)
	f.seedSubscription(t, recipient.ID, "13900000003", true)
	ctx := context.Background()

	if _, err := f.alert.SendOverdueAlerts(ctx); err != nil {
		t.Fatal(err)
	}
	f.alert.timeout = 40 * time.Minute
	if _, err := f.alert.SendOverdueAlerts(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.provider.snapshot()) != 2 {
		t.Fatalf("expected normal+escalated before confirm, got %d", len(f.provider.snapshot()))
	}

	// 对象确认：本周期立即结束，扫描不再发送。
	if _, err := f.recipientSvc.Confirm(ctx, recipient.ID, dto.ConfirmRecipientRequest{Channel: "sms"}); err != nil {
		t.Fatal(err)
	}
	if sent, _ := f.alert.SendOverdueAlerts(ctx); sent != 0 {
		t.Fatalf("scan after confirm sent=%d, want 0", sent)
	}

	// 下一轮再次超时（把确认时间回拨 90 分钟）：必须从普通级重新开始。
	updated, err := f.recipients.Get(ctx, recipient.ID)
	if err != nil {
		t.Fatal(err)
	}
	past := now.Add(-90 * time.Minute)
	updated.LastConfirmedAt = &past
	if err := f.recipients.Update(ctx, updated); err != nil {
		t.Fatal(err)
	}
	f.alert.timeout = time.Hour
	sent, err := f.alert.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sent != 1 {
		t.Fatalf("new cycle sent=%d, want 1", sent)
	}
	if messages := f.provider.snapshot(); messages[len(messages)-1].Kind != constants.SMSKindAlert {
		t.Fatalf("new cycle must restart at normal level, got %q", messages[len(messages)-1].Kind)
	}
}

// 发送失败不记为已发，后续扫描必须重试成功。
func TestAlertServiceFailureIsRetried(t *testing.T) {
	now := time.Now().UTC()
	f := newAlertFixture(t, time.Hour)
	recipient := f.seedRecipient(t, "张阿姨", "13800000003", now.Add(-90*time.Minute), constants.RecipientStatusActive)
	sub := f.seedSubscription(t, recipient.ID, "13900000004", true)
	ctx := context.Background()
	f.provider.failNext = 1

	if sent, err := f.alert.SendOverdueAlerts(ctx); err == nil || sent != 0 {
		t.Fatalf("failing scan: sent=%d err=%v, want 0 and error", sent, err)
	}
	levels, err := f.dispatches.SuccessLevels(ctx, recipient.ID, neverConfirmedCycleKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(levels) != 0 {
		t.Fatalf("failed send must not be recorded as sent, got %v", levels)
	}

	// 下一次扫描重试并成功，且只成功一次。
	sent, err := f.alert.SendOverdueAlerts(ctx)
	if err != nil || sent != 1 {
		t.Fatalf("retry scan sent=%d err=%v", sent, err)
	}
	if messages := f.provider.snapshot(); len(messages) != 1 {
		t.Fatalf("provider calls=%d, want exactly 1 delivered", len(messages))
	}
	if sent, _ = f.alert.SendOverdueAlerts(ctx); sent != 0 {
		t.Fatalf("scan after success sent=%d, want 0", sent)
	}
	_ = sub
}

// 暂停对象不发送；停用的家属订阅也不发送。
func TestAlertServiceSkipsPausedAndInactive(t *testing.T) {
	now := time.Now().UTC()
	f := newAlertFixture(t, time.Hour)
	paused := f.seedRecipient(t, "已暂停", "13800000004", now.Add(-3*time.Hour), constants.RecipientStatusPaused)
	f.seedSubscription(t, paused.ID, "13900000005", true)
	active := f.seedRecipient(t, "启用中", "13800000005", now.Add(-90*time.Minute), constants.RecipientStatusActive)
	f.seedSubscription(t, active.ID, "13900000006", true)
	f.seedSubscription(t, active.ID, "13900000007", false)

	sent, err := f.alert.SendOverdueAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sent != 1 {
		t.Fatalf("sent=%d, want only the one active subscription", sent)
	}
	messages := f.provider.snapshot()
	if messages[0].RecipientPhone != "13900000006" {
		t.Fatalf("alert went to %q", messages[0].RecipientPhone)
	}
}

// 并发/重复扫描整体只生效一次。
func TestAlertServiceConcurrentScansOnce(t *testing.T) {
	now := time.Now().UTC()
	f := newAlertFixture(t, time.Hour)
	recipient := f.seedRecipient(t, "赵阿姨", "13800000006", now.Add(-90*time.Minute), constants.RecipientStatusActive)
	f.seedSubscription(t, recipient.ID, "13900000008", true)
	f.seedSubscription(t, recipient.ID, "13900000009", true)

	const goroutines = 8
	var wg sync.WaitGroup
	var totalSent int64
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			sent, _ := f.alert.SendOverdueAlerts(context.Background())
			atomic.AddInt64(&totalSent, int64(sent))
		}()
	}
	close(start)
	wg.Wait()
	if totalSent != 2 {
		t.Fatalf("concurrent scans total sent=%d, want 2", totalSent)
	}
	if len(f.provider.snapshot()) != 2 {
		t.Fatalf("provider calls=%d, want 2", len(f.provider.snapshot()))
	}
}

// 周期中途新增的家属订阅：仍能收到其尚未收到的各级告警，而已发家属不被重复打扰。
func TestAlertServiceSubscriptionAddedMidCycle(t *testing.T) {
	now := time.Now().UTC()
	f := newAlertFixture(t, 40*time.Minute)
	recipient := f.seedRecipient(t, "钱阿姨", "13800000020", now.Add(-90*time.Minute), constants.RecipientStatusActive)
	firstSub := f.seedSubscription(t, recipient.ID, "13900000010", true)
	ctx := context.Background()

	// 首次扫描：已过两倍时限，老家属同时收到普通与升级告警。
	sent, err := f.alert.SendOverdueAlerts(ctx)
	if err != nil || sent != 2 {
		t.Fatalf("first scan sent=%d err=%v, want 2", sent, err)
	}

	// 升级告警之后新增一个家属订阅。
	newSub := f.seedSubscription(t, recipient.ID, "13900000011", true)
	sent, err = f.alert.SendOverdueAlerts(ctx)
	if err != nil || sent != 2 {
		t.Fatalf("mid-cycle scan sent=%d err=%v, want 2 for new subscription", sent, err)
	}
	messages := f.provider.snapshot()
	var gotToNew []string
	for _, m := range messages {
		if m.FamilySubscriptionID != nil && *m.FamilySubscriptionID == newSub.ID {
			gotToNew = append(gotToNew, m.Kind)
		}
	}
	if len(gotToNew) != 2 {
		t.Fatalf("new subscription levels=%v, want normal+escalated", gotToNew)
	}
	// 老家属仍只有两条，不被重复发送。
	countOld := 0
	for _, m := range messages {
		if m.FamilySubscriptionID != nil && *m.FamilySubscriptionID == firstSub.ID {
			countOld++
		}
	}
	if countOld != 2 {
		t.Fatalf("old subscription messages=%d, want 2", countOld)
	}
}

// 仓储层并发 Claim 同一周期/级别/家属只有一个成功（唯一索引兜底）。
func TestAlertDispatchRepositoryConcurrentClaim(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "claim.db") + "?_busy_timeout=5000"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewAlertDispatchRepository(db)
	now := time.Now().UTC()
	const goroutines = 50
	var wg sync.WaitGroup
	var wins int64
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := repo.Claim(context.Background(), 7, neverConfirmedCycleKey, 9, constants.AlertLevelNormal, now.Add(-time.Minute), now)
			if err == nil {
				atomic.AddInt64(&wins, 1)
			} else if !errors.Is(err, repository.ErrDispatchClaimed) {
				t.Errorf("unexpected claim error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if wins != 1 {
		t.Fatalf("claim wins=%d, want exactly 1", wins)
	}
}

// 状态查询区分普通告警与升级告警。
func TestStatusServiceAlertLevels(t *testing.T) {
	now := time.Now().UTC()
	f := newAlertFixture(t, time.Hour)
	f.seedRecipient(t, "普通告警", "13800000010", now.Add(-90*time.Minute), constants.RecipientStatusActive)
	f.seedRecipient(t, "升级告警", "13800000011", now.Add(-3*time.Hour), constants.RecipientStatusActive)
	safeRecipient := f.seedRecipient(t, "平安", "13800000012", now.Add(-time.Hour), constants.RecipientStatusActive)
	safeRecipient.LastConfirmedAt = &now
	if err := f.recipients.Update(context.Background(), &safeRecipient); err != nil {
		t.Fatal(err)
	}
	statusSvc := NewStatusService(f.recipients, time.Hour)

	items, _, err := statusSvc.List(context.Background(), 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	levels := map[string]RecipientStatus{}
	for _, item := range items {
		levels[item.Recipient.Name] = item
	}
	normal := levels["普通告警"]
	if normal.AlertLevel != constants.AlertLevelNormal || normal.State != "失联" {
		t.Fatalf("normal overdue: level=%q state=%q", normal.AlertLevel, normal.State)
	}
	escalated := levels["升级告警"]
	if escalated.AlertLevel != constants.AlertLevelEscalated {
		t.Fatalf("escalated overdue: level=%q", escalated.AlertLevel)
	}
	safe := levels["平安"]
	if safe.AlertLevel != "" || safe.State != "平安" {
		t.Fatalf("safe: level=%q state=%q", safe.AlertLevel, safe.State)
	}
}
