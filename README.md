# New Relic API Error-Rate Spike Alerts

AWS Lambda that checks New Relic every 5 minutes and posts a Slack card when
an API's error rate is at least 2x the same window yesterday AND last week.
It sends a RESOLVED card when the spike ends. The Lambda keeps no state.

![Critical alert](docs/sample-critical.png)
![Resolved alert](docs/sample-resolved.png)

## How it works
1. EventBridge triggers the Lambda every 5 minutes.
2. The Lambda queries New Relic (NerdGraph) for errors and requests per API.
3. It compares the last 60 minutes with the same window yesterday and last week.
4. It posts alerts to Slack via an incoming webhook.

## Setup
1. Create a Python 3.12 Lambda. Handler: `lambda_function.lambda_handler`.
2. Paste in `lambda_function.py`. Set the timeout to 2-3 minutes.
3. Add these environment variables:

| Variable | Example | Purpose |
|---|---|---|
| `NR_ACCOUNTS` | `account_a:1234567:Account A` | key:accountId:Label, comma separated |
| `NR_API_KEY_ACCOUNT_A` | `NRAK-...` | New Relic User API key for that account |
| `SLACK_WEBHOOK_ACCOUNT_A` | `https://hooks.slack.com/...` | Slack webhook for that account |
| `INCLUDE_APPS` | `my-service` | optional: services without "prod" in the name |
| `DRY_RUN` | `true` | optional: log instead of posting |

4. Test with `{"action":"test","sample":true}`.
5. Create the 5-minute schedule with `{"action":"install"}`.
   The Lambda role needs `events:PutRule`, `events:PutTargets`,
   `lambda:AddPermission`. Or create the EventBridge rule by hand.

## Other test events
- `{"action":"coverage"}` lists services that are not monitored
- `{"action":"uninstall"}` removes the schedule

## Security
Keys and webhooks live only in Lambda environment variables. Never commit them.