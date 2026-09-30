"""AUD-20260930-01: contract-level adversarial regressions shared by both repos."""
import copy
import unittest
import availability_contract_v3 as availability
import relevant_meteorology_registry_v3 as registry
from forecast_lead_identity import (ForecastLeadIdentityError, parse_utc_timestamp,
                                    validate_forecast_lead_identity)


class SharedAuditTests(unittest.TestCase):
    def test_time_precision_never_silently_advances_availability_or_aliases_identity(self):
        for value in ['2026-09-30T06:00:00.0000001Z', '2026-09-30T06:00:00.123456789Z']:
            with self.subTest(value=value), self.assertRaises(ForecastLeadIdentityError):
                parse_utc_timestamp(value, 'availability')
        self.assertEqual(parse_utc_timestamp('2026-09-30T06:00:00.123456000Z','time'),
                         parse_utc_timestamp('2026-09-30T06:00:00.123456Z','time'))

    def test_forecast_lead_cannot_be_rounded_into_an_integer(self):
        with self.assertRaises(ForecastLeadIdentityError):
            validate_forecast_lead_identity(run_time_utc='2026-09-30T06:00:00Z',
                valid_time_utc='2026-09-30T07:00:00.0000001Z', lead_seconds=3600)

    def test_unknown_registry_capability_cannot_become_received_or_an_attempt(self):
        unknown=[(m,s) for m,p in registry.REGISTRY['providers'].items()
                 for s,v in p['capability'].items() if v=='unknown_or_ambiguous']
        self.assertTrue(unknown)
        for model,semantic in unknown:
            original=availability.make_declaration(model=model,semantic_id=semantic,
                                                   observed_at_utc='2026-09-30T06:00:00Z')
            self.assertTrue(availability.validate_declaration(original))
            for status in ['received','fetch_error','not_yet_published','unknown_or_ambiguous']:
                with self.subTest(model=model,semantic=semantic,status=status):
                    altered=copy.deepcopy(original)
                    altered.update(availability_status=status,availability_evidence_type='provider_fetch_attempt',
                                   availability_reason='fabricated runtime evidence',parameter_native='unproven',
                                   field_provider_product='unproven')
                    if status=='received':
                        altered.update(value=1.0,field_available_at_utc='2026-09-30T06:00:00Z')
                    with self.assertRaises(ValueError):
                        availability.validate_declaration(altered)

    def test_valid_received_zero_and_causal_time_are_preserved(self):
        row=availability.make_declaration(model='GFS',semantic_id='convective_precipitation',
            observed_at_utc='2026-09-30T06:00:00Z',runtime_status='received',value=0.0,
            field_available_at_utc='2026-09-30T06:00:00Z',evidence_type='provider_fetch_attempt',
            parameter_native='ACPCP',field_provider_product='gfs_0p25')
        self.assertTrue(availability.validate_declaration(row))
        row['field_available_at_utc']='2026-09-30T06:00:00.0000001Z'
        with self.assertRaises(ValueError): availability.validate_declaration(row)
