#!/usr/bin/env python3
"""Decide whether a scheduled collector run is due from private latest_success state.

The check is intentionally fail-open: if private state cannot be read, acquisition
runs and the reason is emitted explicitly instead of silently skipping data.
"""
from __future__ import annotations
import argparse,base64,json,os,urllib.error,urllib.request
from datetime import datetime,timezone

API="https://api.github.com"
DEFAULT_REPO="janwohlers78/mardorf-kitevorhersage"
FUTURE_TOLERANCE_MINUTES=15

def parse_time(value):
    x=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)

def output(name,value):
    p=os.getenv("GITHUB_OUTPUT")
    if p:
        with open(p,"a",encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    print(f"{name}={value}")

def evaluate_latest_success(stamp,now,max_age_minutes,future_tolerance_minutes=FUTURE_TOLERANCE_MINUTES):
    t=parse_time(stamp)
    age=(now-t).total_seconds()/60
    if age < -float(future_tolerance_minutes):
        return True,"latest_success_timestamp_future_fail_open",age
    age=max(0.0,age)
    due=age>=float(max_age_minutes)
    return due,("last_success_age_exceeds_threshold" if due else "last_success_within_threshold"),age

def state_pointer(kind):
    if kind=="secondary":
        return ("data/inbox/public_collector/transfer_receipts/secondary/latest.json","source_generated_at_utc")
    return (f"data/inbox/public_collector/integrity/{kind}/latest_success.json","generated_at_utc")

def fetch_latest(repo,kind,token):
    from .private_state import due_pointer
    path,stamp_field=due_pointer(repo,kind)
    url=f"{API}/repos/{repo}/contents/{path}?ref=main"
    req=urllib.request.Request(url,headers={
        "Authorization":f"Bearer {token}",
        "Accept":"application/vnd.github+json",
        "X-GitHub-Api-Version":"2022-11-28",
        "User-Agent":"mardorf-data-collector-due-check/1.0",
    })
    with urllib.request.urlopen(req,timeout=20) as r:
        meta=json.loads(r.read().decode("utf-8"))
    raw=base64.b64decode(meta["content"].replace("\n",""))
    value=json.loads(raw.decode("utf-8"))
    if path.startswith('config/cloud_refs/'):
        expected_version='native-acquisition-control-v1' if kind=='models' else 'cloud-collector-ref-v1'
        if (value.get('schema_version')!=1 or value.get('artifact_version')!=expected_version or
                value.get('kind')!=kind or value.get('bundle_ready') is not True or
                value.get('readback_verified') is not True):
            raise ValueError('Cloud acquisition control is not verified')
        if kind=='models' and value.get('metadata',{}).get('channel')!='native-acquisition-only':
            raise ValueError('Explicit native acquisition scope required')
        from mardorf_collector.storage.objects import ObjectRef
        ObjectRef.parse(value['snapshot'])
        when=datetime.fromisoformat(value['generated_at_utc'].replace('Z','+00:00'))
        if when.tzinfo is None:
            raise ValueError('Cloud acquisition control lacks timezone')
    # Retain the unchanged caller's stamp contract, including secondary batches.
    _,original_field=state_pointer(kind)
    return {**value,original_field:value.get(stamp_field)}

def model_retry_due(runs, now, cooldown_minutes=120, *, last_verified_success=None):
    """Bound failure retries without treating failures as successful acquisition.

    Explicit manual acquisition bypasses the routine due gate. The latest failed
    main-branch attempt controls cooldown; active jobs are handled by
    the watchdog concurrency guard. Unknown evidence fails open visibly.
    """
    terminal = [r for r in runs if r.get('head_branch') == 'main'
                and r.get('status') == 'completed'
                and r.get('conclusion') in ('failure', 'cancelled', 'timed_out')]
    if not terminal:
        return True, 'no_terminal_model_attempt'
    latest = max(terminal, key=lambda r: parse_time(r['updated_at']))
    # A successful due-check-only workflow is not successful acquisition.
    # Only the independently verified delivery timestamp can clear a failure.
    if last_verified_success is not None and parse_time(last_verified_success) >= parse_time(latest['updated_at']):
        return True, 'verified_delivery_after_failed_attempt'
    age = (now-parse_time(latest['updated_at'])).total_seconds()/60
    if age < 0:
        return True, 'future_retry_timestamp_fail_open'
    return age >= cooldown_minutes, ('model_failure_cooldown' if age < cooldown_minutes
                                    else 'model_failure_cooldown_elapsed')


def fetch_model_attempts(token):
    repository = 'janwohlers78/mardorf-data-collector'
    url = f'{API}/repos/{repository}/actions/workflows/collect-models.yml/runs?branch=main&per_page=20'
    request = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'mardorf-routine-retry/1.0'})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read())['workflow_runs']


def daily_weather_due(value, now):
    """A narrow Berlin recovery slot; no late forecast masquerades as on-time."""
    from zoneinfo import ZoneInfo
    local=now.astimezone(ZoneInfo('Europe/Berlin'))
    if not (7,35) <= (local.hour,local.minute) < (7,50):
        return False, 'outside_daily_recovery_slot'
    if value is not None:
        from mardorf_collector.storage.objects import ObjectRef
        try:
            ref=ObjectRef.parse(value['snapshot'])
            generated=parse_time(value['generated_at_utc'])
            valid=(value['artifact_version']=='integrated-daily-report-control-v1'
                   and value['readback_verified'] is True and value['bundle_ready'] is True
                   and value['metadata']['payload_sha256']==ref.sha256
                   and generated <= now and generated.astimezone(local.tzinfo).date()==local.date()
                   and (generated.astimezone(local.tzinfo).hour,generated.astimezone(local.tzinfo).minute)>=(7,35))
            if valid: return False, 'today_integrated_report_available'
        except (ValueError,KeyError,TypeError):
            pass
    return True, 'today_integrated_report_missing_or_unverified'


def fetch_daily_report(repo, token):
    url=f'{API}/repos/{repo}/contents/config/cloud_refs/integrated_daily_report_v1.json?ref=main'
    request=urllib.request.Request(url,headers={
        'Authorization':f'Bearer {token}', 'Accept':'application/vnd.github+json',
        'X-GitHub-Api-Version':'2022-11-28', 'User-Agent':'mardorf-daily-recovery/1.0'})
    try:
        with urllib.request.urlopen(request,timeout=20) as response:
            result=json.loads(response.read())
        return json.loads(base64.b64decode(result['content']))
    except urllib.error.HTTPError as exc:
        if exc.code==404:return None
        raise



def fetch_daily_forecast_attempts(repo, token):
    url=f'{API}/repos/{repo}/actions/workflows/wp06-prospective-daily.yml/runs?branch=main&per_page=20'
    request=urllib.request.Request(url,headers={
        'Authorization':f'Bearer {token}', 'Accept':'application/vnd.github+json',
        'X-GitHub-Api-Version':'2022-11-28', 'User-Agent':'mardorf-daily-recovery/1.0'})
    with urllib.request.urlopen(request,timeout=20) as response:
        return json.loads(response.read())['workflow_runs']


def daily_recovery_decision(value, runs, now):
    due,reason=daily_weather_due(value,now)
    if reason=='outside_daily_recovery_slot':return due,reason
    if runs is None:
        return True,'daily_report_job_state_unavailable_fail_open'
    busy={'queued','in_progress','waiting','pending','requested'}
    main_runs=[r for r in runs if r.get('head_branch')=='main']
    if any(r.get('status') in busy for r in main_runs):
        return False,'private_daily_forecast_queued_or_running'
    if not due:
        event_id=str(value.get('metadata',{}).get('event_id',''))
        report_run=next((r for r in main_runs if str(r.get('id'))==event_id),None)
        if report_run is None:
            return True,'daily_report_run_not_verified_fail_open'
        if report_run.get('status')!='completed' or report_run.get('conclusion')!='success':
            return True,'daily_report_run_not_successful'
    return due,reason


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--kind",required=True,choices=("svg","models","wunstorf","etnw","secondary","daily_weather"))
    ap.add_argument("--max-age-minutes",required=True,type=float)
    args=ap.parse_args()
    token=os.getenv("PRIVATE_REPO_TOKEN","")
    repo=os.getenv("PRIVATE_REPO",DEFAULT_REPO)
    now=datetime.now(timezone.utc)

    if args.kind == 'daily_weather':
        value=None; runs=None
        # Avoid network calls outside the only useful recovery window.
        active,_=daily_weather_due(None,now)
        if active and token:
            try:value=fetch_daily_report(repo,token)
            except (ValueError,KeyError,TypeError,OSError):pass
            try:runs=fetch_daily_forecast_attempts(repo,token)
            except (ValueError,KeyError,TypeError,OSError):pass
        due,reason=daily_recovery_decision(value,runs,now)
        output('due','true' if due else 'false');output('reason',reason)
        output('age_minutes','not_applicable');output('threshold_minutes','Berlin0735_to0750')
        return
    due=True;reason="unknown";age=None;stamp=None
    if not token:
        reason="private_repo_token_not_configured_fail_open"
    else:
        try:
            latest=fetch_latest(repo,args.kind,token)
            _,stamp_field=state_pointer(args.kind)
            stamp=latest.get(stamp_field)
            if not stamp:
                reason="latest_success_has_no_generated_at_fail_open"
            else:
                due,reason,age=evaluate_latest_success(stamp,now,args.max_age_minutes)
        except urllib.error.HTTPError as e:
            reason=f"private_latest_success_http_{e.code}_fail_open"
        except Exception as e:
            reason=f"private_latest_success_read_{type(e).__name__}_fail_open"

    if due and args.kind == 'models' and token:
        try:
            allowed, retry_reason = model_retry_due(fetch_model_attempts(token), now, last_verified_success=stamp)
            if not allowed:
                due=False; reason=retry_reason
        except (ValueError, KeyError, TypeError, OSError):
            reason += '_retry_state_unavailable_fail_open'
    output("due","true" if due else "false")
    output("reason",reason)
    output("last_success_generated_at_utc",stamp or "none")
    output("age_minutes",f"{age:.2f}" if age is not None else "unknown")
    output("threshold_minutes",f"{args.max_age_minutes:.2f}")

if __name__=="__main__":
    main()
