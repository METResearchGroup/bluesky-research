# Slack Webhook Setup Guide

## Quick Setup Instructions

### 1. Get Your Slack Webhook URL

1. Go to [Slack Apps](https://api.slack.com/apps)
2. Create a new app or use an existing one
3. Enable "Incoming Webhooks"
4. Add a webhook to your workspace
5. Copy the webhook URL

### 2. Recommended: Configure via Docker Secret

This project loads the Slack webhook securely from a Docker secret and references it in Alertmanager via `api_url_file`.

#### 2.1 Create the secret file

```bash
mkdir -p secrets
echo "https://hooks.slack.com/services/YOUR/ACTUAL/WEBHOOK/URL" > secrets/slack_webhook_url
```

Ensure this file is not committed to version control.

#### 2.2 Ensure docker-compose mounts the secret

In `docker-compose.monitoring.yml`, the `alertmanager` service must declare the secret and the bottom of the file must define it:

```yaml
services:
  alertmanager:
    # ...
    secrets:
      - slack_webhook_url

secrets:
  slack_webhook_url:
    file: ./secrets/slack_webhook_url
```

#### 2.3 Reference the secret in Alertmanager config

In `alertmanager/alertmanager.yml`, each Slack receiver should reference the secret file using `api_url_file`:

```yaml
receivers:
  - name: 'slack-notifications'
    slack_configs:
      - channel: '#redis-monitoring'
        api_url_file: '/run/secrets/slack_webhook_url'
        send_resolved: true
        title: '{{ template "slack.title" . }}'
        text: '{{ template "slack.text" . }}'
```

Repeat `api_url_file` for other Slack receivers if defined (e.g., `slack-critical`, `slack-warning`, `slack-info`).

### 3. Restart the Stack

```bash
docker compose -f docker-compose.monitoring.yml down
docker compose -f docker-compose.monitoring.yml up -d
```

### 4. Test the Integration

Run the validation script:

```bash
cd redis_testing
python 07_monitoring_validation.py
```

## Requirements Summary (Integrated)

The Slack integration is designed to support proactive alerting for the Redis monitoring stack. The core requirements are summarized below.

### Functional Requirements
- Critical Alerts (immediate): Redis down, memory > 95%, p95 latency > 50ms, high error rates (> 5%), buffer overflow detected
- Warning Alerts (5-minute delay): memory > 85%, p95 latency > 20ms, throughput < 500 ops/sec, connection count > 100
- Info Alerts (daily): daily performance summary, memory trends, throughput stats
- Delivery: Dedicated `#redis-monitoring` channel with rich, actionable messages
- Escalation: Auto-escalate after 15 minutes without acknowledgment; support acknowledgment via Slack reactions

### Technical Requirements
- Alertmanager with Slack receiver using `api_url_file: /run/secrets/slack_webhook_url`
- Prometheus alert rules covering critical metrics
- Message templates for different severities
- Rate limiting (max 10 alerts/min) and retry logic (3 attempts, exponential backoff)

### Security Requirements
- Webhook over HTTPS; URL stored in Docker secret (`secrets/slack_webhook_url`)
- Read-only access to monitoring data; no sensitive data in alert messages

## Implementation Plan (Integrated)

### Phase 1: Infrastructure Setup
1) Alertmanager configuration: `alertmanager/alertmanager.yml` with Slack receiver referencing `/run/secrets/slack_webhook_url`
2) Prometheus alert rules: `alerts/redis_alerts.yml`
3) Slack app: create webhook and target `#redis-monitoring`

### Phase 2: Alerts
- Critical: Redis down, memory pressure (>95%), high latency (>50ms), error rate
- Warning: memory >85%, latency >20ms, throughput, connections
- Info: daily summaries and trend notifications

### Phase 3: Testing & Validation
- Simulate conditions and verify delivery formatting, escalation, acknowledgment, and rate-limiting

## Configuration References

### Alertmanager Receiver (example)
```yaml
receivers:
  - name: 'slack-notifications'
    slack_configs:
      - channel: '#redis-monitoring'
        api_url_file: '/run/secrets/slack_webhook_url'
        send_resolved: true
        title: '{{ template "slack.title" . }}'
        text: '{{ template "slack.text" . }}'
```

### Prometheus Alert Rules (examples)
```yaml
groups:
  - name: redis_alerts
    rules:
      - alert: RedisDown
        expr: redis_up == 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "Redis instance is down"

      - alert: RedisMemoryHigh
        expr: redis_memory_used_bytes / redis_memory_max_bytes > 0.95
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Redis memory usage is high (>95%)"
```

### Message Templates (Alertmanager)
```yaml
# alertmanager/templates/slack.tmpl
{{ define "slack.title" }}
[{{ .Status | toUpper }}] {{ .CommonLabels.alertname }}
{{ end }}

{{ define "slack.text" }}
{{ range .Alerts }}
*Alert:* {{ .Annotations.summary }}
*Severity:* {{ .Labels.severity }}
*Started:* {{ .StartsAt | since }}
{{ end }}
{{ end }}
```

## Operational Notes
- Rotate the webhook URL periodically; update `secrets/slack_webhook_url` and restart stack
- Validate alerts with `python redis_testing/07_monitoring_validation.py`
- Prefer Docker secrets over environment variables for credentials

## Webhook URL Format

Your webhook URL should look like:

```text
hxxps://hooks.slack.com/services/TXXXXXXXX/BXXXXXXXX/XXXXXXXXXXXXXXXX (obfuscated)
```

## Security Notes

- Keep your webhook URL secret
- Do not commit it to version control
- Prefer Docker secrets over environment variables for sensitive values
- Consider using Slack app tokens for more secure integrations
