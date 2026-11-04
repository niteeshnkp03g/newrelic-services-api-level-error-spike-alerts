
"""
Error_rate_Alerts_Api_level  --  per-API error-rate spike alerts (New Relic -> Slack)

ONE method for EVERY service, in BOTH accounts (Account A, Account B):

    SELECT sum(apm.service.transaction.error.count['count']) AS e,
           count(apm.service.transaction.duration)           AS t
    FROM Metric WHERE transactionType IN ('Web','Other') AND <prod services>
    FACET entity.name, transactionName  TIMESERIES 5 minutes

    error rate = errors / requests, per API (service + transaction name).

An alert opens when, for a window (the last 60 minutes by default), the error rate is at least
2x the SAME window yesterday AND last week, there are enough errors to matter, and the
window was not already spiking 5 minutes ago (so no duplicate alerts: the Lambda keeps no
state). It resolves when the spike ends. Every window opens and resolves on its own.

Which services are covered: every entity.name containing "prod" (any case), plus the names in
INCLUDE_APPS. Run {"action": "coverage"} to list any service with traffic that is NOT covered.

Environment variables
  NEW_RELIC_API_KEY_A / NEW_RELIC_API_KEY_B   User API keys (Account A / Account B)
  NR_ACCOUNT_ID_A / NR_ACCOUNT_ID_B           New Relic account IDs
  NR_ACCOUNT_ID      optional, e.g. 1111111,2222222 (limit accounts)      NR_REGION=EU optional
  SLACK_WEBHOOK      (or SLACK_WEBHOOK_A / SLACK_WEBHOOK_B)
  WARN_RATIO=2   CRIT_RATIO=2        spike = rate >= ratio x reference   (both 2 = one level)
  WINDOWS_MIN=60                     windows to check (minutes, multiples of 5; e.g. 5,30,60)
  HISTORY_COMPARE=both               both (higher of yesterday/last week) | any (lower)
  MIN_ERRORS                         override: min errors in a window (default 10 for 5 min, 15 for 30 min, 30 for 60 min)
  RATE_FLOOR=0.0001                  only avoids dividing by 0 (0.01%); a reference of 0% still alerts
  INCLUDE_APPS                       extra services without "prod" in the name (comma list)
  EXCLUDED_APPS                      services to ignore (comma list)
  MAX_ALERTS_PER_MSG=20              cards per Slack message (max 24); extra alerts go in further messages
  DRY_RUN=true                       log instead of posting to Slack

Test events (Lambda console -> Test)
  {"action":"test","sample":true}    check keys + Slack and post sample cards
  {"action":"coverage"}              list services with traffic that are not covered
  {"action":"send","account":"account_a","alerts":[{...}]}   post cards from JSON
  {"action":"install"} / {"action":"uninstall"}          create/remove the 5-minute EventBridge rule
"""
import json
import logging
import os
import re
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

log = logging.getLogger()
log.setLevel(logging.INFO)

IST = timezone(timedelta(hours=5, minutes=30))
ACCOUNTS = {
    "account_a": {"id": int(os.environ.get("NR_ACCOUNT_ID_A", "1111111")), "label": "Account A"},
    "account_b": {"id": int(os.environ.get("NR_ACCOUNT_ID_B", "2222222")), "label": "Account B"},
}

STEP = 300            # one slice = 5 minutes
DELAY_MIN = 2         # skip the newest 2 minutes: New Relic data arrives a little late
RULE_NAME = "newrelic-api-error-rate-spike-every-5-min"
LAGS = {"day": 86400, "week": 604800}          # seconds
RANK = {None: 0, "WARNING": 1, "CRITICAL": 2}

# Services that have traffic but no "prod" in the name (env INCLUDE_APPS replaces this list).
DEFAULT_INCLUDE_APPS = (
    "example-payment-webhook-consumer,"
    "example-priority-webhook-consumer,"
    "example-website-backend,EXAMPLE_MODEL_SERVICE,EXAMPLE_COLLECTIONS_SERVICE")

METRIC_SELECT = ("sum(apm.service.transaction.error.count['count']) AS e, "
                 "count(apm.service.transaction.duration) AS t")


def _includes():
    return [x.strip() for x in os.environ.get("INCLUDE_APPS", DEFAULT_INCLUDE_APPS).split(",")
            if x.strip() and re.fullmatch(r"[A-Za-z0-9._() -]+", x.strip())]


def apps_filter(field="entity.name"):
    """NRQL condition selecting every prod service (+ INCLUDE_APPS)."""
    extra = _includes()
    if not extra:
        return f"{field} RLIKE '(?i).*prod.*'"
    names = ", ".join(f"'{x}'" for x in extra)
    return f"({field} RLIKE '(?i).*prod.*' OR {field} IN ({names}))"


def is_covered(name):
    return "prod" in (name or "").lower() or name in _includes()


# --------------------------------------------------------------------------- #
# Name cleanup: collapse IDs so one API is one row
# --------------------------------------------------------------------------- #
TXN_PREFIXES = (
    "WebTransaction/Go/", "WebTransaction/Action/", "WebTransaction/Function/",
    "WebTransaction/Uri/", "WebTransaction/SpringController/",
    "WebTransaction/Expressjs/", "WebTransaction/Nodejs/",
    "WebTransaction/", "OtherTransaction/Go/", "OtherTransaction/",
)
EMAIL = re.compile(r"[^/\s]+@[^/\s]+")
UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-")


def clean_api(name):
    if not name:
        return "(unnamed transaction)"
    for p in TXN_PREFIXES:
        if name.startswith(p):
            name = name[len(p):]
            break
    if not name:
        return "(unnamed transaction)"
    name = EMAIL.sub("{email}", name)
    parts = []
    for seg in name.split("/"):
        if seg.isdigit() or UUID.match(seg) or (len(seg) >= 6 and any(c.isdigit() for c in seg)):
            parts.append("{id}")
        else:
            parts.append(seg)
    return "/".join(parts)


# --------------------------------------------------------------------------- #
# New Relic NerdGraph
# --------------------------------------------------------------------------- #
def _api_key(account):
    own = {"account_a": "NEW_RELIC_API_KEY_A", "account_b": "NEW_RELIC_API_KEY_B"}[account]
    return (os.environ.get(own) or os.environ.get(f"NR_API_KEY_{account.upper()}")
            or os.environ.get("NR_API_KEY"))


def _active_accounts():
    wanted = {x.strip() for x in os.environ.get("NR_ACCOUNT_ID", "").split(",") if x.strip()}
    if not wanted:
        return list(ACCOUNTS)
    return [a for a, v in ACCOUNTS.items() if str(v["id"]) in wanted]


def _endpoint():
    return ("https://api.eu.newrelic.com/graphql"
            if os.environ.get("NR_REGION", "US").upper() == "EU"
            else "https://api.newrelic.com/graphql")


def run_nrql(account, nrql):
    gql = ("query($id: Int!, $q: Nrql!) { actor { account(id: $id) "
           "{ nrql(query: $q, timeout: 90) { results } } } }")
    body = json.dumps({"query": gql,
                       "variables": {"id": ACCOUNTS[account]["id"], "q": nrql}}).encode()
    req = urllib.request.Request(
        _endpoint(), data=body, method="POST",
        headers={"Content-Type": "application/json", "API-Key": _api_key(account)})
    with urllib.request.urlopen(req, timeout=100) as r:
        data = json.loads(r.read())
    if data.get("errors"):
        raise RuntimeError(f"NerdGraph error ({account}): {data['errors'][0].get('message')}")
    return data["data"]["actor"]["account"]["nrql"]["results"]


def _nrql_metric(since, until):
    """THE query: errors and requests per service + transaction, in 5-minute slices."""
    return (f"SELECT {METRIC_SELECT} FROM Metric "
            f"WHERE transactionType IN ('Web', 'Other') AND {apps_filter()} "
            f"FACET entity.name, transactionName LIMIT MAX TIMESERIES 5 minutes "
            f"SINCE {since} UNTIL {until}")


def _q(v):
    """Escape a value for use inside single quotes in NRQL."""
    return str(v).replace("\\", "\\\\").replace("'", "\\'")


def _slack_safe(v):
    """Names come from New Relic data: strip anything that could break out of Slack formatting
    or trigger @mentions / links (backticks, <, >, &)."""
    return re.sub(r"[`<>&]", "", str(v))[:300]


RAW = defaultdict(set)   # (service, cleaned api) -> raw transaction names (for the span lookup)


def _fold(rows, start, nb):
    """Flat TIMESERIES rows -> {(service, api): ([errors per slice], [requests per slice])}."""
    out = defaultdict(lambda: ([0.0] * nb, [0.0] * nb))
    for r in rows:
        facet = r.get("facet") or [r.get("entity.name"), r.get("transactionName")]
        app, name = facet[0], facet[1] if len(facet) > 1 else ""
        api = clean_api(name)
        i = (int(r.get("beginTimeSeconds", 0)) - start) // STEP
        if 0 <= i < nb:
            out[(app, api)][0][i] += r.get("e", 0) or 0
            out[(app, api)][1][i] += r.get("t", 0) or 0
        if len(RAW[(app, api)]) < 20:
            RAW[(app, api)].add(name or "")
    return out


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
def _windows():
    raw = os.environ.get("WINDOWS_MIN", "60")
    ws = sorted({int(x) for x in raw.split(",") if x.strip().isdigit() and int(x) % 5 == 0 and int(x) > 0})
    return ws or [60]


def _pick():
    return "any" if os.environ.get("HISTORY_COMPARE", "both").lower() == "any" else "both"


def _rate_floor():
    return float(os.environ.get("RATE_FLOOR", "0.0001"))


def _min_errors(w):
    if os.environ.get("MIN_ERRORS"):
        return int(os.environ["MIN_ERRORS"])
    return {5: 10, 10: 10, 30: 15, 60: 30}.get(w, max(10, w // 2))


def _ratios():
    warn = float(os.environ.get("WARN_RATIO", "2"))
    crit = float(os.environ.get("CRIT_RATIO", "2"))
    return warn, max(crit, warn)


# --------------------------------------------------------------------------- #
# Fetch: today + same slices yesterday + same slices last week (3 queries per account)
# --------------------------------------------------------------------------- #
def _rate(e, t):
    return (sum(e) / sum(t)) if sum(t) > 0 else 0.0


def fetch_account(account):
    ws = _windows()
    nb = max(ws) // 5 + 1
    end_ts = (int(time.time()) - DELAY_MIN * 60) // STEP * STEP
    start_ts = end_ts - nb * STEP

    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {"today": ex.submit(run_nrql, account, _nrql_metric(start_ts, end_ts))}
        for n, lag in LAGS.items():
            futs[n] = ex.submit(run_nrql, account, _nrql_metric(start_ts - lag, end_ts - lag))
        raw = {k: f.result() for k, f in futs.items()}

    avail = [n for n in LAGS if raw[n]]          # a lag with no data at all is skipped
    if not avail:
        raise RuntimeError("no history found for yesterday or last week; check data retention")
    pickfn = max if _pick() == "both" else min

    today = _fold(raw["today"], start_ts, nb)
    refs = {n: _fold(raw[n], start_ts - LAGS[n], nb) for n in avail}
    zero = ([0.0] * nb, [0.0] * nb)

    merged = {}
    for key, (te, tt) in today.items():
        if sum(te) <= 0:                         # no errors in the whole period: nothing to alert
            continue
        wins = {}
        for w in ws:
            k = w // 5
            info = {}
            for tag, (a, b) in {"cur": (nb - k, nb), "prev": (nb - 1 - k, nb - 1)}.items():
                e, t = sum(te[a:b]), sum(tt[a:b])
                lag = {n: _rate(refs[n].get(key, zero)[0][a:b], refs[n].get(key, zero)[1][a:b])
                       for n in avail}
                info[tag] = {"errors": e, "requests": t, "rate": (e / t if t > 0 else 0.0),
                             "ref": pickfn(lag.values()), "lag": lag}
            wins[w] = info
        merged[key] = {"windows": wins, "t1": end_ts}
    return merged


# --------------------------------------------------------------------------- #
# Spike detection (stateless)
# --------------------------------------------------------------------------- #
def _level(rate, ref, errors, w):
    if errors < _min_errors(w):
        return None
    warn, crit = _ratios()
    ratio = rate / max(ref, _rate_floor())
    if ratio >= crit:
        return "CRITICAL"
    if ratio >= warn:
        return "WARNING"
    return None


def detect(merged, now=None):
    """
    Each window opens/resolves independently.
      open     = spiking now and was not one slice (5 min) ago (or WARNING -> CRITICAL)
      resolved = was spiking one slice ago and is not now
    A spike that simply continues produces no event.
    """
    now = now or datetime.now(IST)
    default_t1 = (int(now.timestamp()) - DELAY_MIN * 60) // STEP * STEP
    excluded = {x.strip() for x in os.environ.get("EXCLUDED_APPS", "").split(",") if x.strip()}
    floor = _rate_floor()

    events = []
    for (app, api), info in merged.items():
        if app in excluded:
            continue
        t1 = info.get("t1") or default_t1
        for w, d in sorted(info["windows"].items()):
            cur, prev = d["cur"], d["prev"]
            cl = _level(cur["rate"], cur["ref"], cur["errors"], w)
            pl = _level(prev["rate"], prev["ref"], prev["errors"], w)

            def fields(x):
                return {"current": int(x["errors"]), "requests": int(x["requests"]),
                        "baseline": round(x["ref"], 4), "ratio": x["rate"] / max(x["ref"], floor),
                        "rates": (x["rate"], x["lag"].get("day"), x["lag"].get("week"))}

            base = {"app": app, "api": api, "window_min": w}
            if cl and (pl is None or (pl == "WARNING" and cl == "CRITICAL")):
                events.append({**base, "state": "open", **fields(cur), "level": cl,
                               "escalated": pl is not None, "t0": t1 - w * 60, "t1": t1,
                               "opened": datetime.fromtimestamp(t1 - STEP, IST), "closed": None})
            elif cl is None and pl is not None:
                events.append({**base, "state": "resolved", **fields(prev), "level": pl,
                               "escalated": False, "t0": t1 - STEP - w * 60, "t1": t1 - STEP,
                               "opened": None, "closed": datetime.fromtimestamp(t1 - STEP, IST)})
    events.sort(key=lambda e: (e["state"] != "open", e["level"] != "CRITICAL",
                               -e["ratio"], -e["current"]))
    return events


def find_span(account, ev):
    """For a resolved event: (opened, closed) IST datetimes of the latest run of spiking slices."""
    raw = sorted(RAW.get((ev["app"], ev["api"]), set()))[:20]
    if not raw:
        return None, None
    names = ", ".join("'" + _q(n) + "'" for n in raw)
    q = (f"SELECT {METRIC_SELECT} FROM Metric WHERE transactionType IN ('Web', 'Other') "
         f"AND entity.name = '{_q(ev['app'])}' AND transactionName IN ({names}) "
         f"TIMESERIES 5 minutes SINCE {DELAY_MIN + 420} minutes ago UNTIL {DELAY_MIN} minutes ago")
    try:
        buckets = sorted(run_nrql(account, q), key=lambda b: b["beginTimeSeconds"])
    except Exception:  # noqa: BLE001 - never block the resolve message for a nicety
        log.exception("span lookup failed")
        return None, None
    warn, _ = _ratios()
    limit = warn * max(ev["baseline"], _rate_floor())

    def spiking(b):
        e, t = b.get("e") or 0, b.get("t") or 0
        return e >= 3 and e / max(t, 1) >= limit

    first = last = None
    for b in reversed(buckets):
        if last is None:
            if spiking(b):
                last = first = b
        elif spiking(b):
            first = b
        else:
            break
    if last is None:
        return None, None
    end = last.get("endTimeSeconds") or last["beginTimeSeconds"] + STEP
    return (datetime.fromtimestamp(first["beginTimeSeconds"], tz=IST),
            datetime.fromtimestamp(end, tz=IST))


# --------------------------------------------------------------------------- #
# Slack: one card per API (top-level blocks, so Slack never folds them behind "Show more")
# --------------------------------------------------------------------------- #
def _fmt(dt):
    return dt.astimezone(IST).strftime("%b %d, %Y %H:%M IST") if dt else "n/a"


def _pct(v):
    return "n/a" if v is None else f"{v * 100:.1f}%"


def build_message(events):
    cap = int(os.environ.get("MAX_ALERTS_PER_MSG", "20"))
    warn, crit = _ratios()
    blocks = []
    for e in events[:cap]:
        resolved = e["state"] == "resolved"
        if resolved:
            icon, word = "\u2705", "RESOLVED"
        elif e["level"] == "CRITICAL":
            icon, word = "\U0001F6A8", "CRITICAL"
        else:
            icon, word = "\u26A0\uFE0F", "WARNING"
        title = f"{icon} {word} - API_Error_Spike" + (" (worsened)" if e["escalated"] else "")
        x = crit if e["level"] == "CRITICAL" else warn
        t, y, lw = (_pct(v) for v in e["rates"])
        lines = [
            f"\U0001F5A5\uFE0F *Service:* `{_slack_safe(e['app'])}`",
            f"\U0001F517 *Path / API:* `{_slack_safe(e['api'])}`",
            # whole value in ONE code box, same style as every other line
            f"\U0001F4C8 *Error rate:* `{t} today · {y} yesterday · {lw} last week → {e['ratio']:.1f}x`",
            f"\U0001F552 *Opened (IST):* `{_fmt(e['opened'])}`",
            f"\u23F1\uFE0F *Closed (IST):* `{_fmt(e['closed']) if resolved else 'Still Open'}`",
        ]
        if blocks:
            blocks.append({"type": "divider"})
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": f"`{title}`\n" + "\n".join(lines)}})
    if len(events) > cap:
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
                                                    "text": f"_...and {len(events) - cap} more_"}})
    return {"blocks": blocks}


def post_slack(account, message):
    if os.environ.get("DRY_RUN", "").lower() == "true":
        log.info("DRY_RUN slack message for %s: %s", account, json.dumps(message))
        return 200
    suffix = {"account_a": "A", "account_b": "B"}[account]
    url = os.environ.get(f"SLACK_WEBHOOK_{suffix}") or os.environ.get("SLACK_WEBHOOK")
    if not url:
        raise RuntimeError(f"no Slack webhook for {account}")
    req = urllib.request.Request(url, data=json.dumps(message).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.status


# --------------------------------------------------------------------------- #
# Self-scheduling (EventBridge rule created by the Lambda itself)
# --------------------------------------------------------------------------- #
def install_schedule(context, events=None, lam=None):
    if events is None or lam is None:
        import boto3
        events = events or boto3.client("events")
        lam = lam or boto3.client("lambda")
    arn = context.invoked_function_arn
    expr = os.environ.get("SCHEDULE_EXPRESSION", "rate(5 minutes)")
    rule_arn = events.put_rule(Name=RULE_NAME, ScheduleExpression=expr, State="ENABLED",
                               Description="Triggers Error_rate_Alerts_Api_level")["RuleArn"]
    try:
        lam.add_permission(FunctionName=arn, StatementId=RULE_NAME, Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rule_arn)
    except Exception as exc:  # noqa: BLE001 - already exists on re-run
        if "ResourceConflictException" not in type(exc).__name__ and "already exists" not in str(exc):
            raise
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": arn}])
    return {"installed": RULE_NAME, "schedule": expr}


def uninstall_schedule(context, events=None, lam=None):
    if events is None or lam is None:
        import boto3
        events = events or boto3.client("events")
        lam = lam or boto3.client("lambda")
    events.remove_targets(Rule=RULE_NAME, Ids=["1"])
    events.delete_rule(Name=RULE_NAME)
    try:
        lam.remove_permission(FunctionName=context.invoked_function_arn, StatementId=RULE_NAME)
    except Exception:  # noqa: BLE001
        pass
    return {"uninstalled": RULE_NAME}


# --------------------------------------------------------------------------- #
# Coverage + self-test + manual send
# --------------------------------------------------------------------------- #
def coverage():
    """Per account: services with traffic in the last 24 h, and which of them are NOT covered."""
    out = {}
    for account in _active_accounts():
        try:
            rows = run_nrql(account,
                            "SELECT count(apm.service.transaction.duration) AS t FROM Metric "
                            "WHERE entity.name RLIKE '.+' AND transactionType IN ('Web', 'Other') FACET entity.name "
                            "SINCE 1 day ago LIMIT MAX")
            seen = {(r.get("facet") if isinstance(r.get("facet"), str) else r["facet"][0]): r.get("t", 0)
                    for r in rows}
            out[account] = {
                "services_with_traffic": len(seen),
                "covered": sorted(n for n in seen if is_covered(n)),
                "NOT_covered": sorted(n for n in seen if not is_covered(n)),
            }
        except Exception as exc:  # noqa: BLE001
            out[account] = {"error": str(exc)}
    log.info("Coverage: %s", json.dumps(out))
    return out


def _sample_events():
    t = datetime.now(IST).replace(second=0, microsecond=0)
    mk = dict(requests=0, escalated=False, closed=None, opened=t, state="open")
    return [
        {**mk, "app": "SAMPLE-prod-service", "api": "POST /api/v1/sample/critical", "window_min": 30,
         "current": 54, "requests": 600, "baseline": 0.02, "ratio": 4.5, "level": "CRITICAL",
         "rates": (0.09, 0.02, 0.01)},
        {**mk, "app": "SAMPLE-prod-service", "api": "PATCH /api/v1/sample/worsened", "window_min": 60,
         "current": 90, "requests": 1500, "baseline": 0.03, "ratio": 2.0, "level": "CRITICAL",
         "escalated": True, "rates": (0.06, 0.03, None)},
        {**mk, "app": "SAMPLE-prod-service", "api": "GET /api/v1/sample/resolved", "window_min": 30,
         "current": 324, "requests": 2000, "baseline": 0.07, "ratio": 2.3, "level": "CRITICAL",
         "state": "resolved", "opened": t - timedelta(minutes=40), "closed": t - timedelta(minutes=5),
         "rates": (0.16, 0.07, 0.06)},
    ]


def self_test(send_slack=True, sample=False):
    report = {}
    for account in _active_accounts():
        res = {}
        try:
            rows = run_nrql(account, "SELECT count(apm.service.transaction.duration) AS t FROM Metric "
                                     f"WHERE {apps_filter()} SINCE 5 minutes ago")
            n = int(rows[0].get("t", 0)) if rows else 0
            res["newrelic"] = f"OK ({n} prod requests in the last 5 min)" if n else \
                "WARNING: query worked but returned 0 (wrong account/key?)"
        except Exception as exc:  # noqa: BLE001
            res["newrelic"] = f"FAILED: {exc}"
        if send_slack:
            try:
                label = ACCOUNTS[account]["label"]
                test_text = (f":white_check_mark: [{label}] TEST message from "
                             "Error_rate_Alerts_Api_level. Not a real alert.")
                res["slack"] = f"OK (HTTP {post_slack(account, {'text': test_text})})"
                if sample:
                    m = build_message(_sample_events())
                    m["blocks"][0]["text"]["text"] = "`SAMPLE (not real)`  " + m["blocks"][0]["text"]["text"]
                    res["slack_sample"] = f"OK (HTTP {post_slack(account, m)})"
            except Exception as exc:  # noqa: BLE001
                res["slack"] = f"FAILED: {exc}"
        report[account] = res
    log.info("Self-test: %s", json.dumps(report))
    return report


def _parse_ist(v):
    if not v:
        return None
    if str(v).lower() == "now":
        return datetime.now(IST).replace(second=0, microsecond=0)
    return datetime.strptime(str(v), "%Y-%m-%d %H:%M").replace(tzinfo=IST)


def events_from_json(alerts):
    """{"service","api","window_min","errors","requests","rates":[today,yday,lastweek in %],
        "level","state","worsened","opened","closed"}"""
    out = []
    for a in alerts:
        r = [(None if v is None else float(v) / 100) for v in (a.get("rates") or [None] * 3)]
        r += [None] * (3 - len(r))
        errs, reqs = int(a.get("errors", 0)), int(a.get("requests", 0))
        ref = r[1] if r[1] is not None else (r[2] or 0)
        out.append({
            "app": a["service"], "api": a["api"], "window_min": int(a.get("window_min", 30)),
            "state": a.get("state", "open"), "level": a.get("level", "CRITICAL").upper(),
            "escalated": bool(a.get("worsened", False)), "current": errs, "requests": reqs,
            "rates": tuple(r), "baseline": ref,
            "ratio": float(a.get("ratio", (r[0] or 0) / max(ref, _rate_floor()))),
            "opened": _parse_ist(a.get("opened")), "closed": _parse_ist(a.get("closed"))})
    return out


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #
def lambda_handler(event, context):
    action = (event or {}).get("action") if isinstance(event, dict) else None
    if action == "send":
        account = str(event.get("account", "account_a")).lower()
        if account not in ACCOUNTS:
            raise ValueError(f"account must be one of {list(ACCOUNTS)}")
        status = post_slack(account, build_message(events_from_json(event["alerts"])))
        return {"sent": len(event["alerts"]), "account": account, "slack": f"HTTP {status}"}
    if action == "test":
        return self_test(send_slack=event.get("slack", True), sample=event.get("sample", False))
    if action == "coverage":
        return coverage()
    if action == "install":
        return install_schedule(context)
    if action == "uninstall":
        return uninstall_schedule(context)

    summary = {}
    for account in _active_accounts():
        try:
            merged = fetch_account(account)
            events = detect(merged)
            for e in events:
                if e["state"] == "resolved":
                    opened, closed = find_span(account, e)
                    e["opened"] = opened
                    e["closed"] = closed or e["closed"]
            cap = max(1, min(int(os.environ.get("MAX_ALERTS_PER_MSG", "20")), 24))
            for i in range(0, len(events), cap):     # more alerts than one message holds -> more messages, none dropped
                post_slack(account, build_message(events[i:i + cap]))
            summary[account] = {"apis_with_errors": len(merged),
                                "new_spikes": sum(e["state"] == "open" for e in events),
                                "resolved": sum(e["state"] == "resolved" for e in events)}
        except Exception as exc:  # noqa: BLE001 - one account failing must not stop the other
            log.exception("Account %s failed", account)
            summary[account] = {"error": str(exc)}
    log.info("Run summary: %s", json.dumps(summary))
    return summary