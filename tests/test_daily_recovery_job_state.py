import unittest
from datetime import datetime,timezone
from mardorf_collector.runtime.check_collection_due import daily_recovery_decision

class DailyRecoveryStateTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,10,10,5,40,tzinfo=timezone.utc)
        self.report=dict(artifact_version='integrated-daily-report-control-v1',readback_verified=True,bundle_ready=True,
            generated_at_utc='2026-10-10T05:38:00Z',snapshot=dict(schema_version=1,key='reports/a',sha256='a'*64,bytes=10),
            metadata=dict(payload_sha256='a'*64,event_id='123'))
    def run_state(self,status,conclusion=None):return dict(id=123,head_branch='main',status=status,conclusion=conclusion)
    def test_active_private_forecast_suppresses_redundant_original_acquisition(self):
        for status in ('queued','in_progress','waiting','pending','requested'):
            self.assertEqual(daily_recovery_decision(None,[self.run_state(status)],self.now),(False,'private_daily_forecast_queued_or_running'))
    def test_saved_report_requires_actual_successful_bound_run(self):
        self.assertFalse(daily_recovery_decision(self.report,[self.run_state('completed','success')],self.now)[0])
        for conclusion in ('failure','cancelled','timed_out'):
            self.assertEqual(daily_recovery_decision(self.report,[self.run_state('completed',conclusion)],self.now),(True,'daily_report_run_not_successful'))
        self.assertTrue(daily_recovery_decision(self.report,[],self.now)[0])
    def test_failed_run_allows_repair_and_unknown_state_does_not_claim_freshness(self):
        self.assertTrue(daily_recovery_decision(None,[self.run_state('completed','failure')],self.now)[0])
        self.assertEqual(daily_recovery_decision(self.report,None,self.now),(True,'daily_report_job_state_unavailable_fail_open'))
        branch=self.run_state('in_progress');branch['head_branch']='development'
        self.assertTrue(daily_recovery_decision(None,[branch],self.now)[0])
    def test_outside_slot_still_never_recovers_late(self):
        self.assertEqual(daily_recovery_decision(self.report,None,datetime(2026,10,10,6,0,tzinfo=timezone.utc)),(False,'outside_daily_recovery_slot'))
