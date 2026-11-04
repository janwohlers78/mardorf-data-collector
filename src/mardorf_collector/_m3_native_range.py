"""Per-run payload admission at the Zarr boundary; no unbounded shard read."""
COORDINATES={'init_time','lead_time','valid_time','ensemble_member','latitude','longitude','expected_forecast_length','ingested_forecast_length','spatial_ref'}

class Budget:
    def __init__(self,protocol):
        self.allowed=set(protocol['allowed_weather_keys'].values());self.limits=protocol['resource_plan']
        self.weather_bytes=0;self.metadata_bytes=0;self.pending=0;self.weather_requests=0;self.metadata_requests=0;self.denied=0

    def before(self,key,request):
        key=str(key)
        metadata=(key in ('zarr.json','.zmetadata','.zattrs','.zgroup') or key.endswith(('/zarr.json','/.zarray','/.zattrs','/.zgroup')) or key.split('/',1)[0] in COORDINATES)
        if metadata:
            self.metadata_requests+=1
            if self.metadata_requests>self.limits['max_metadata_requests']:raise ValueError('metadata request budget exhausted')
            return ('metadata',self.limits['metadata_coordinate_payload_budget_bytes'])
        try:
            if key not in self.allowed:raise ValueError('weather key outside two approved u/v shards')
            if self.weather_requests>=self.limits['max_weather_requests']:raise ValueError('weather request budget exhausted')
            if request is None:raise ValueError('full weather shard read prohibited')
            if type(request).__name__=='RangeByteRequest':
                start=request.start;end=request.end
                if type(start) is not int or type(end) is not int or not 0<=start<end:raise ValueError('strict byte range')
                maximum=end-start
                if maximum>self.limits['range_payload_max_bytes']:raise ValueError('weather range exceeds one encoded inner chunk')
            elif type(request).__name__=='SuffixByteRequest':
                maximum=request.suffix
                if type(maximum) is not int or not 0<maximum<=self.limits['suffix_index_max_bytes']:raise ValueError('bounded shard index suffix')
            else:raise ValueError('only bounded range/suffix weather requests allowed')
            if self.weather_bytes+self.pending+maximum>self.limits['received_weather_payload_budget_bytes']:raise ValueError('weather payload byte budget exhausted')
        except ValueError:
            self.denied+=1;raise
        self.pending+=maximum;self.weather_requests+=1
        return ('weather',maximum)

    def after(self,ticket,length):
        kind,maximum=ticket
        if type(length) is not int or length<0:raise ValueError('actual returned payload bytes')
        if kind=='weather':self.pending-=maximum
        if length>maximum:raise ValueError('store returned more than admitted byte request')
        if kind=='weather':self.weather_bytes+=length
        else:
            self.metadata_bytes+=length
            if self.metadata_bytes>self.limits['metadata_coordinate_payload_budget_bytes']:raise ValueError('metadata coordinate payload budget exhausted')

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
