package service

import (
	"context"
	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestStatusServiceList(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewRecipientRepository(db)
	alertRepo := repository.NewAlertRecordRepository(db)
	now := time.Now().UTC()
	paused := model.CareRecipient{Name: "P", Phone: "100001", CareFrequency: "daily", CareStartAt: now.Add(-time.Hour), Status: "Paused"}
	recent := model.CareRecipient{Name: "R", Phone: "100002", CareFrequency: "daily", CareStartAt: now.Add(-time.Hour), Status: "Active", LastConfirmedAt: &now}
	never := model.CareRecipient{Name: "N", Phone: "100003", CareFrequency: "daily", CareStartAt: now.Add(-time.Minute), Status: "Active"}
	overdue := model.CareRecipient{Name: "O", Phone: "100004", CareFrequency: "daily", CareStartAt: now.Add(-48 * time.Hour), Status: "Active"}
	escalated := model.CareRecipient{Name: "E", Phone: "100005", CareFrequency: "daily", CareStartAt: now.Add(-72 * time.Hour), Status: "Active"}
	for _, r := range []model.CareRecipient{paused, recent, never, overdue, escalated} {
		item := r
		if err := repo.Create(context.Background(), &item); err != nil {
			t.Fatal(err)
		}
		if item.Name == "O" {
			overdue = item
		}
		if item.Name == "E" {
			escalated = item
		}
	}
	// 当前周期内 O 已发普通告警，E 普通与升级告警均已送达；另造一条历史周期记录，不应影响状态。
	for _, record := range []model.AlertRecord{
		{CareRecipientID: overdue.ID, FamilySubscriptionID: 1, CycleRef: alertCycleRef(overdue), Level: constants.AlertLevelNormal, SentAt: now},
		{CareRecipientID: escalated.ID, FamilySubscriptionID: 1, CycleRef: alertCycleRef(escalated), Level: constants.AlertLevelNormal, SentAt: now},
		{CareRecipientID: escalated.ID, FamilySubscriptionID: 1, CycleRef: alertCycleRef(escalated), Level: constants.AlertLevelEscalated, SentAt: now},
		{CareRecipientID: never.ID, FamilySubscriptionID: 1, CycleRef: now.Add(-24 * time.Hour), Level: constants.AlertLevelEscalated, SentAt: now},
	} {
		item := record
		claimed, err := alertRepo.Claim(context.Background(), &item)
		if err != nil {
			t.Fatal(err)
		}
		if !claimed {
			t.Fatal("seed alert record was not claimed")
		}
	}
	svc := NewStatusService(repo, alertRepo, time.Hour)
	items, total, err := svc.List(context.Background(), 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("total=%d len=%d, want 5", total, len(items))
	}
	states := map[string]string{}
	levels := map[string]string{}
	for _, item := range items {
		states[item.Recipient.Name] = item.State
		levels[item.Recipient.Name] = item.AlertLevel
	}
	if states["P"] != "已暂停" {
		t.Fatalf("P state=%q", states["P"])
	}
	if states["R"] != "平安" {
		t.Fatalf("R state=%q", states["R"])
	}
	if states["N"] != "待确认" {
		t.Fatalf("N state=%q", states["N"])
	}
	if states["O"] != "失联" {
		t.Fatalf("O state=%q", states["O"])
	}
	if levels["O"] != constants.AlertLevelNormal {
		t.Fatalf("O alert_level=%q, want normal", levels["O"])
	}
	if levels["E"] != constants.AlertLevelEscalated {
		t.Fatalf("E alert_level=%q, want escalated", levels["E"])
	}
	for _, name := range []string{"P", "R", "N"} {
		if levels[name] != constants.AlertLevelNone {
			t.Fatalf("%s alert_level=%q, want none", name, levels[name])
		}
	}
}
