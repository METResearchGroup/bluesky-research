# Slack Webhook Setup Guide

## Quick Setup Instructions

### 1. Get Your Slack Webhook URL

1. Go to https://api.slack.com/apps
2. Create a new app or use existing one
3. Enable "Incoming Webhooks"
4. Add webhook to your workspace
5. Copy the webhook URL

### 2. Configure the Webhook

**Option A: Direct Configuration**
Edit `alertmanager/alertmanager.yml`:
```yaml
global:
  slack_api_url: 'YOUR_ACTUAL_WEBHOOK_URL_HERE'
```

**Option B: Environment Variable (Recommended)**
1. Create `.env` file in this directory:
```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/ACTUAL/WEBHOOK/URL
```

2. Update `docker-compose.monitoring.yml` to use the environment variable:
```yaml
environment:
  - SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
```

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
```
https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

## Security Notes

- Keep your webhook URL secret
- Don't commit it to version control
- Use environment variables in production
- Consider using Slack app tokens for more secure integration
