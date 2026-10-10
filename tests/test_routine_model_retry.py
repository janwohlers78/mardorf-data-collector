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
