import copy
from datetime import datetime, timedelta, timezone
import io
import unittest
from mardorf_collector.wp17 import regimes_v1 as r
from mardorf_collector.storage.objects import ObjectRef


def tables(start=datetime(2025,1,1,tzinfo=timezone.utc), count=8):
    left=[];right=[]
    for i in range(count):
        d=start+timedelta(hours=3*i);h=int((d-datetime(1979,1,1,tzinfo=timezone.utc)).total_seconds()/3600)
        eof=1 if datetime(1979,1,11,tzinfo=timezone.utc)<=d<datetime(2020,1,1,tzinfo=timezone.utc) and d.hour%6==0 else 0
        base=f'{h} {d:%Y%m%d_%H}'
        left.append(base+f' {eof} 2 0');right.append(base+' -1 0.1 0.2 0.3 0.4 0.5 0.6')
    return ('header\n'+'\n'.join(left)).encode(),('\n'.join(right)).encode(),r.stamp(start),r.stamp(start+timedelta(hours=3*count))


class RegimeArchiveTests(unittest.TestCase):
    def parse(self, t):return r.parse_tables(t[0],t[1],start_utc=t[2],end_utc=t[3])
    def test_eof_sentinel_not_no_regime(self):
        rows=self.parse(tables());self.assertIsNone(rows[0]['eof_class']);self.assertEqual(rows[0]['lifecycle_class'],0)
    def test_eof_training_scope(self):
        rows=self.parse(tables(datetime(2018,1,1,tzinfo=timezone.utc)));self.assertEqual(rows[0]['eof_class'],1);self.assertIsNone(rows[1]['eof_class'])
    def test_indices_remain_signed_not_probabilities(self):self.assertEqual(self.parse(tables())[0]['AT'],-1)
    def test_disordered_timestamps_rejected(self):
        t=list(tables());t[0]=t[0].replace(b'20250101_03',b'20250101_00')
        with self.assertRaises(ValueError):self.parse(t)
    def test_historical_hour_contradiction(self):
        t=list(tables());first=t[1].split(b' ',1)[0];t[1]=t[1].replace(first,str(int(first)+1).encode(),1)
        with self.assertRaises(ValueError):self.parse(t)
    def test_short_table_rejected(self):
        t=list(tables());t[1]=b'\n'.join(t[1].splitlines()[:-1])
        with self.assertRaises(ValueError):self.parse(t)
    def test_nonfinite_index_rejected(self):
        t=list(tables());t[1]=t[1].replace(b'0.1',b'nan')
        with self.assertRaises(ValueError):self.parse(t)
    def test_invalid_class_rejected(self):
        t=list(tables());t[0]=t[0].replace(b' 2 0',b' 2 8')
        with self.assertRaises(ValueError):self.parse(t)
    def test_eof_outside_scope_rejected(self):
        t=list(tables());t[0]=t[0].replace(b' 0 2 0',b' 1 2 0')
        with self.assertRaises(ValueError):self.parse(t)
    def test_arrow_cold_values_and_schema(self):
        rows=self.parse(tables());body=r.parquet(rows);ref=ObjectRef('weather/regimes/v1/partitions/year/2025/'+r.sha(body),r.sha(body),len(body))
        class Backend:
            def get_bytes(self,asked):self.asked=asked;return body
        self.assertEqual(r.read_partition(Backend(),ref.json(),year=2025,expected_rows=8),rows)
    def test_wrong_partition_year_rejected(self):
        rows=self.parse(tables());body=r.parquet(rows);ref=ObjectRef('weather/regimes/v1/partitions/year/2024/'+r.sha(body),r.sha(body),len(body))
        class Backend:
            def get_bytes(self,asked):return body
        with self.assertRaises(ValueError):r.read_partition(Backend(),ref.json(),year=2024,expected_rows=8)
    def test_corrupted_original(self):
        with self.assertRaises(ValueError):r.bound(b'bad',{'bytes':3,'sha256':'0'*64})


if __name__=='__main__':unittest.main()
