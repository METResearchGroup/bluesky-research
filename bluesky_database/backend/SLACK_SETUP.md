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
