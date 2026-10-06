import math,struct,unittest
from unittest.mock import Mock
from mardorf_collector.wp13.native_grid_v1 import coordinates,point_report,_cache

def prefix(uuid='a'*32):
    def u(n):return struct.pack('>I',n)
    def text(s):
        b=s.encode();return u(len(b))+b+b'\0'*((-len(b))%4)
    def attributes(d):return u(12)+u(len(d))+b''.join(text(k)+u(2)+u(len(v.encode()))+v.encode()+b'\0'*((-len(v.encode()))%4) for k,v in d.items())
    start=b'CDF\x01'+u(0)+u(10)+u(1)+text('cell')+u(2)+attributes({'uuidOfHGrid':uuid})+u(11)+u(2)
    def variables(offset):return b''.join(text(k)+u(1)+u(0)+attributes({'units':'radian'})+u(6)+u(16)+u(offset+i*16) for i,k in enumerate(['clon','clat']))
    offset=len(start)+len(variables(0));return start+variables(offset)+struct.pack('>dddd',math.radians(9.327636527),math.radians(9.347928045),math.radians(52.502699112),math.radians(52.492985847))

class NativeGridTests(unittest.TestCase):
    def setUp(self):
        _cache.clear();self.identity={'number_of_grid_used':'47','uuid_of_horizontal_grid':'a'*32,'number_of_data_points':2}
    def test_true_native_cell_does_not_require_regular_grid_parity(self):
        _cache[(47,'a'*32,2)]=coordinates(prefix(),self.identity)
        proof=point_report({'latitude':52.502697,'longitude':9.327637},self.identity,None)
        self.assertTrue(proof['verified']);self.assertLess(proof['coordinate_difference_m'],1)
        self.assertFalse(proof['requested_nearest_cell_claimed'])
        self.assertFalse(point_report({'latitude':52.5,'longitude':9.34},self.identity,None)['verified'])
    def test_uuid_count_and_truncated_coordinates_fail_closed(self):
        from mardorf_collector.wp13.native_grid_v1 import NeedMore
        with self.assertRaisesRegex(ValueError,'UUID'):coordinates(prefix('b'*32),self.identity)
        with self.assertRaisesRegex(ValueError,'schema'):coordinates(prefix(),dict(self.identity,number_of_data_points=3))
        with self.assertRaises(NeedMore):coordinates(prefix()[:-2],self.identity)
    def test_stream_stops_after_coordinate_prefix_and_reuses_grid_across_targets(self):
        import bz2
        response=Mock();response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        response.iter_content.return_value=iter([bz2.compress(prefix()),b'not consumed'])
        session=Mock();session.get.return_value=response
        self.assertTrue(point_report({'latitude':52.502697,'longitude':9.327637},self.identity,session)['verified'])
        point_report({'latitude':52.492985847,'longitude':9.347928045},self.identity,session)
        session.get.assert_called_once()

if __name__=='__main__':unittest.main()
