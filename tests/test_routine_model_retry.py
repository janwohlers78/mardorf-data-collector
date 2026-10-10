from datetime import datetime, timezone, timedelta
import unittest
from mardorf_collector.runtime.check_collection_due import model_retry_due

class RetryTests(unittest.TestCase):
    def test_failed_transfer_is_not_repeated_every_watchdog_heartbeat(self):
        now=datetime(2026,10,10,8,tzinfo=timezone.utc)
        row=dict(head_branch='main',status='completed',conclusion='cancelled',updated_at=now.isoformat())
        self.assertEqual(model_retry_due([row],now+timedelta(minutes=20)),(False,'model_failure_cooldown'))
        self.assertTrue(model_retry_due([row],now+timedelta(minutes=120))[0])
        row['conclusion']='success'
        self.assertTrue(model_retry_due([row],now)[0])
    def test_other_branches_and_future_clocks_do_not_silently_block(self):
        now=datetime(2026,10,10,8,tzinfo=timezone.utc)
        row=dict(head_branch='experiment',status='completed',conclusion='failure',updated_at=now.isoformat())
        self.assertTrue(model_retry_due([row],now)[0])
        row['head_branch']='main';row['updated_at']=(now+timedelta(hours=1)).isoformat()
        self.assertTrue(model_retry_due([row],now)[0])

class DailyRecoveryTests(unittest.TestCase):
    def test_no_network_or_late_attempt_outside_recovery_slot(self):
        from mardorf_collector.runtime.check_collection_due import daily_weather_due
        for hour,minute in [(5,34),(5,50),(6,0)]:
            now=datetime(2026,10,10,hour,minute,tzinfo=timezone.utc)
            self.assertFalse(daily_weather_due(None,now)[0])
        self.assertTrue(daily_weather_due(None,datetime(2026,10,10,5,40,tzinfo=timezone.utc))[0])
    def test_verified_today_report_suppresses_but_yesterday_does_not(self):
        from mardorf_collector.runtime.check_collection_due import daily_weather_due
        now=datetime(2026,10,10,5,40,tzinfo=timezone.utc)
        value=dict(artifact_version='integrated-daily-report-control-v1',readback_verified=True,bundle_ready=True,
            generated_at_utc='2026-10-10T05:38:00Z',snapshot=dict(schema_version=1,key='reports/a',sha256='a'*64,bytes=10),
            metadata=dict(payload_sha256='a'*64))
        self.assertFalse(daily_weather_due(value,now)[0])
        value['generated_at_utc']='2026-10-09T05:38:00Z'
        self.assertTrue(daily_weather_due(value,now)[0])
