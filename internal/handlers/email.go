package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// SendEmailJob sends an email
type SendEmailJob struct {
	logger *slog.Logger
}

func NewSendEmailJob(logger *slog.Logger) *SendEmailJob {
	return &SendEmailJob{logger: logger}
}

func (h *SendEmailJob) Type() string {
	return "send_email"
}

func (h *SendEmailJob) Execute(ctx context.Context, payload json.RawMessage) error {
	var data struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
		Body    string `json:"body"`
	}

	if err := json.Unmarshal(payload, &data); err != nil {
		return err
	}

	h.logger.Info("sending email", "to", data.To, "subject", data.Subject)

	// Simulate email sending (replace with actual email service)
	time.Sleep(2 * time.Second)

	h.logger.Info("email sent successfully", "to", data.To)
	return nil
}

// GenerateReportJob generates a report
type GenerateReportJob struct {
	logger *slog.Logger
}

func NewGenerateReportJob(logger *slog.Logger) *GenerateReportJob {
	return &GenerateReportJob{logger: logger}
}

func (h *GenerateReportJob) Type() string {
	return "generate_report"
}

func (h *GenerateReportJob) Execute(ctx context.Context, payload json.RawMessage) error {
	var data struct {
		ReportType string `json:"report_type"`
		UserID     string `json:"user_id"`
	}

	if err := json.Unmarshal(payload, &data); err != nil {
		return err
	}

	h.logger.Info("generating report", "type", data.ReportType, "user_id", data.UserID)

	// Simulate report generation
	time.Sleep(3 * time.Second)

	h.logger.Info("report generated successfully", "type", data.ReportType)
	return nil
}
