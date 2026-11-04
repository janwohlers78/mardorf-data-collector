"""Strict point extraction contract for separately admitted native IFS fields.

No I/O or decoder is hidden here. Callers must independently bind producer,
object and all codecs before passing decoded arrays. Output is a forecast
covariate, never a local measurement or an hourly observational wind mean.
"""
from datetime import datetime, timedelta, timezone
import math


class QualificationError(ValueError):
    pass


FIELDS = {
    '10u': ('m s**-1', '10', 'heightAboveGround'),
    '10v': ('m s**-1', '10', 'heightAboveGround'),
    '2t': ('K', '2', 'heightAboveGround'),
    'sp': ('Pa', '0', 'surface'),
    'msl': ('Pa', '0', None),
}


def forecast_times(header):
    """Validate native initialization, valid time and instantaneous lead."""
    if header.get('stepType') != 'instant':
        raise QualificationError('instantaneous_forecast_covariate_required')
    if header.get('indicatorOfUnitOfTimeRange') != 'h':
        raise QualificationError('explicit_hour_forecast_period_required')
    try:
        run = datetime.strptime(str(header['dataDate']) + f"{int(header['dataTime']):04d}", '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
        valid = datetime.strptime(str(header['validityDate']) + f"{int(header['validityTime']):04d}", '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
        lead = float(header['endStep'])
        start = float(header['startStep'])
    except (KeyError, TypeError, ValueError):
        raise QualificationError('native_time_metadata_invalid') from None
    if not math.isfinite(lead) or lead < 0 or lead != start or run + timedelta(hours=lead) != valid:
        raise QualificationError('native_forecast_time_identity_failed')
    return run, lead, valid


def field_contract(header):
  …1751 tokens truncated…es>self.limits['metadata_coordinate_payload_budget_bytes']:raise ValueError('metadata coordinate payload budget exhausted')

    def failed(self,ticket):
        if ticket[0]=='weather':self.pending-=ticket[1]

    def receipt(self):
        if self.pending:raise ValueError('uncompleted store reads')
        return dict(weather_payload_bytes_received=self.weather_bytes,metadata_coordinate_payload_bytes_received=self.metadata_bytes,weather_requests=self.weather_requests,metadata_requests=self.metadata_requests,denied_requests=self.denied,all_weather_reads_bounded_ranges=True,full_shards_read=False,physical_wire_bytes_or_retries_measured=False)


def profile_cost(metadata,init_index,lead_indices,lat_indices,lon_indices):
    """Count actual inner chunks and exact outer shard keys from native metadata."""
    import itertools,math
    if metadata['zarr_format']!=3 or metadata['data_type']!='float32' or metadata['dimension_names']!=['init_time','ensemble_member','lead_time','latitude','longitude']:raise ValueError('native u/v layout')
    outer=metadata['chunk_grid']['configuration']['chunk_shape'];codecs=metadata['codecs']
    if len(codecs)!=1 or codecs[0]['name']!='sharding_indexed':raise ValueError('qualified native sharding codec')
    cfg=codecs[0]['configuration'];inner=cfg['chunk_shape']
    if cfg['index_location']!='end' or metadata['chunk_key_encoding']!={'name':'default','configuration':{'separator':'/'}}:raise ValueError('qualified shard index/key layout')
    if cfg['codecs']!=[{'name':'bytes','configuration':{'endian':'little'}},{'name':'blosc','configuration':{'cname':'zstd','clevel':3,'shuffle':'shuffle','typesize':4,'blocksize':0}}]:raise ValueError('qualified lossless inner codecs')
    if cfg['index_codecs']!=[{'name':'bytes','configuration':{'endian':'little'}},{'name':'crc32c'}]:raise ValueError('qualified shard index codecs')
    if len(outer)!=5 or len(inner)!=5 or len(metadata['shape'])!=5 or any(type(x) is not int or x<=0 for x in outer+inner) or any(o%i for o,i in zip(outer,inner)):raise ValueError('exact regular shard/inner shapes')
    selection=[[init_index],list(range(31)),lead_indices,lat_indices,lon_indices]
    if any(any(type(x) is not int or not 0<=x<metadata['shape'][i] for x in values) for i,values in enumerate(selection)):raise ValueError('native selection coordinates')
    inner_coordinates=[sorted({v//c for v in values}) for values,c in zip(selection,inner)]
    outer_coordinates=[sorted({v//c for v in values}) for values,c in zip(selection,outer)]
    return dict(inner_chunks=math.prod(map(len,inner_coordinates)),inner_decoded_bytes=math.prod(inner)*4,outer_logical_bytes=math.prod(outer)*4,index_bytes=math.prod(o//i for o,i in zip(outer,inner))*16+4,outer_key_suffixes=['c/'+'/'.join(map(str,key)) for key in itertools.product(*outer_coordinates)])


def bounded_store(store,budget,WrapperStore):
    class RangedOnly(WrapperStore):
        async def get(self,key,prototype,byte_range=None):
            ticket=budget.before(key,byte_range)
            try:value=await self._store.get(key,prototype=prototype,byte_range=byte_range)
            except BaseException:budget.failed(ticket);raise
            budget.after(ticket,0 if value is None else len(value));return value
        async def get_partial_values(self,prototype,key_ranges):
            return [await self.get(key,prototype,request) for key,request in key_ranges]
        async def _get_many(self,requests):
            for key,prototype,request in requests:yield key,await self.get(key,prototype,request)
        async def get_ranges(self,key,byte_ranges,*,prototype,max_concurrency=None,max_gap_bytes=None,max_coalesced_bytes=None):
            # Deliberately no coalescing: every exact range passes the existing guard.
            for index,request in enumerate(byte_ranges):
                value=await self.get(key,prototype,request)
                if value is None:raise ExceptionGroup('guarded range key absent',[FileNotFoundError(str(key))])
                yield [(index,value)]
        @property
        def _supports_sync_io(self):return False
        def get_sync(self,*args,**kwargs):raise PermissionError('synchronous source reads disabled')
        def get_ranges_sync(self,*args,**kwargs):raise PermissionError('synchronous source ranges disabled')
        async def set(self,*args,**kwargs):raise PermissionError('readonly native source')
        async def delete(self,*args,**kwargs):raise PermissionError('readonly native source')
        async def clear(self,*args,**kwargs):raise PermissionError('readonly native source')
        async def set_if_not_exists(self,*args,**kwargs):raise PermissionError('readonly native source')
        async def set_partial_values(self,*args,**kwargs):raise PermissionError('readonly native source')
    result=RangedOnly(store)
    if not result.read_only:raise ValueError('actual readonly source store required')
    return result
