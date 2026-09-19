package model

import "time"

// AlertDispatch 记录一个确认周期内对某个家属订阅的某一级告警的发送状态。
// 唯一索引 (care_recipient_id, cycle_key, family_subscription_id, alert_level)
// 保证重复或并发扫描在同一周期、同一级别上最多成功发送一次。
type AlertDispatch struct {
	ID                   uint       `gorm:"primaryKey" json:"id"`
	CareRecipientID      uint       `gorm:"not null;uniqueIndex:uniq_alert_dispatch,priority:1;index" json:"care_recipient_id"`
	CycleKey             string     `gorm:"size:40;not null;uniqueIndex:uniq_alert_dispatch,priority:2" json:"cycle_key"`
	FamilySubscriptionID uint       `gorm:"not null;uniqueIndex:uniq_alert_dispatch,priority:3;index" json:"family_subscription_id"`
	AlertLevel           string     `gorm:"size:16;not null;uniqueIndex:uniq_alert_dispatch,priority:4" json:"alert_level"`
	Status               string     `gorm:"size:16;not null;index" json:"status"`
	SMSLogID             *uint      `json:"sms_log_id,omitempty"`
	AttemptAt            time.Time  `gorm:"not null" json:"attempt_at"`
	SentAt               *time.Time `json:"sent_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}
