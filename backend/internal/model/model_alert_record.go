package model

import "time"

// AlertRecord 记录一次已成功送达家属的分级告警。
// 唯一索引 uk_alert_cycle 约束同一确认周期（CycleRef）内同一订阅同一级别只记一次，
// 用于超时告警的分级去重闭环：发送失败不留记录可重试，并发扫描由唯一索引保证只生效一次。
type AlertRecord struct {
	ID                   uint      `gorm:"primaryKey" json:"id"`
	CareRecipientID      uint      `gorm:"not null;uniqueIndex:uk_alert_cycle,priority:1" json:"care_recipient_id"`
	FamilySubscriptionID uint      `gorm:"not null;uniqueIndex:uk_alert_cycle,priority:2" json:"family_subscription_id"`
	CycleRef             time.Time `gorm:"not null;uniqueIndex:uk_alert_cycle,priority:3" json:"cycle_ref"`
	Level                string    `gorm:"size:16;not null;uniqueIndex:uk_alert_cycle,priority:4" json:"level"`
	SentAt               time.Time `gorm:"not null" json:"sent_at"`
	CreatedAt            time.Time `json:"created_at"`
}
