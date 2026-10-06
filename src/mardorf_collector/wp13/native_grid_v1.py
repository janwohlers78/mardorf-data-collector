"""Bounded shared native-grid coordinate proof, without regular-grid assumptions."""
from array import array
import bz2
import hashlib
import io
import math
import struct
import sys

URLS={47:'https://opendata.dwd.de/weather/lib/cdo/icon_grid_0047_R19B07_L.nc.bz2'}
MAX_BYTES=32*1024**2
_cache={}
class NeedMore(ValueError):pass

def coordinates(body,identity):
    f=io.BytesIO(body)
    def read(n):
        if not 0<=n<=MAX_BYTES:raise ValueError('Grid prefix size')
        b=f.read(n)
        if len(b)!=n:raise NeedMore('Grid prefix incomplete')
        return b
    def u32():return struct.unpack('>I',read(4))[0]
    def name():
        n=u32()
        if n>256:raise ValueError('Grid header name bound')
        value=read(n).decode();read((-n)%4);return value
    sizes={1:1,2:1,3:2,4:4,5:4,6:8}
    def attributes():
        tag,count=u32(),u32()
        if tag not in (0,12) or count>128:raise ValueError('Grid attributes bound')
        result={}
        for _ in range(count):
            key=name();kind,count=u32(),u32()
            if kind not in sizes or count>1024:raise ValueError('Grid attribute type/bound')
            n=sizes[kind]*count;value=read(n);read((-n)%4)
            result[key]=value.decode() if kind==2 else value
        return result
    magic=read(4)
    if magic not in (b'CDF\x01',b'CDF\x02'):raise ValueError('Explicit NetCDF3 grid definition required')
    records=u32();tag,count=u32(),u32()
    if records or tag!=10 or count>32:raise ValueError('Static grid dimensions required')
    dims=[(name(),u32()) for _ in range(count)];attrs=attributes();tag,count=u32(),u32()
    if tag!=11 or count>128:raise ValueError('Grid variables bound')
    variables={}
    for _ in range(count):
        key=name();n=u32()
        if n>8:raise ValueError('Grid dimension bound')
        indexes=[u32() for _ in range(n)]
        if any(i>=len(dims) for i in indexes):raise ValueError('Grid dimension identity')
        att=attributes();kind,size=u32(),u32();offset=struct.unpack('>Q' if magic[-1]==2 else '>I',read(8 if magic[-1]==2 else 4))[0]
        variables[key]=(indexes,att,kind,size,offset)
    uuid=attrs.get('uuidOfHGrid','').replace('-','').lower()
    if uuid!=str(identity['uuid_of_horizontal_grid']).replace('-','').lower():raise ValueError('Native grid UUID mismatch')
    points=int(identity['number_of_data_points']);arrays={};end=f.tell()
    if not 0<points<=1024**2:raise ValueError('Native grid point count bound')
    for key in ('clat','clon'):
        indexes,att,kind,size,offset=variables[key]
        if (kind!=6 or att.get('units')!='radian' or len(indexes)!=1
            or dims[indexes[0]]!=('cell',points) or size!=points*8 or offset+size>MAX_BYTES):
            raise ValueError('Native coordinate schema/count/units')
        if offset+size>len(body):raise NeedMore('Coordinate span incomplete')
        values=array('d');values.frombytes(body[offset:offset+size])
        if sys.byteorder=='little':values.byteswap()
        arrays[key]=values;end=max(end,offset+size)
    return arrays,hashlib.sha256(body[:end]).hexdigest()

def point_report(returned,identity,session):
    number=int(identity['number_of_grid_used']);uuid=identity['uuid_of_horizontal_grid']
    if number not in URLS:raise ValueError('Registered native grid definition required')
    key=(number,uuid,int(identity['number_of_data_points']))
    if key not in _cache:
        decoder=bz2.BZ2Decompressor();body=bytearray();wire=0
        with session.get(URLS[number],stream=True,timeout=30) as response:
            response.raise_for_status()
            for chunk in response.iter_content(65536):
                wire+=len(chunk)
                if wire>MAX_BYTES:raise ValueError('Native grid compressed prefix bound')
                body.extend(decoder.decompress(chunk,max_length=MAX_BYTES-len(body)))
                if len(body)>=MAX_BYTES:raise ValueError('Native grid decoded prefix bound')
                try:_cache[key]=coordinates(body,identity);break
                except NeedMore:pass
            else:raise ValueError('Native grid coordinate prefix unavailable')
    arrays,checksum=_cache[key];lat,lon=math.radians(float(returned['latitude'])),math.radians(float(returned['longitude']))
    if not math.isfinite(lat+lon) or abs(lat)>math.pi/2 or abs(lon)>math.pi:raise ValueError('Finite native point required')
    cosine=math.cos(lat);best=None
    for i,(a,b) in enumerate(zip(arrays['clat'],arrays['clon'])):
        if not math.isfinite(a+b):raise ValueError('Native grid coordinate invalid')
        distance=(a-lat)**2+((b-lon)*cosine)**2
        if best is None or distance<best[0]:best=(distance,i,a,b)
    _,i,a,b=best
    distance=6371000*2*math.asin(min(1,math.sqrt(math.sin((a-lat)/2)**2+math.cos(a)*math.cos(lat)*math.sin((b-lon)/2)**2)))
    return dict(verified=distance<=1,method='provider_returned_point_matched_to_native_eps_cell_v1',
        grid_definition_url=URLS[number],coordinate_prefix_sha256=checksum,grid_uuid=uuid,
        native_cell_index=i,native_cell_coordinate=dict(latitude=math.degrees(a),longitude=math.degrees(b)),
        coordinate_difference_m=distance,tolerance_m=1,
        requested_nearest_cell_claimed=False,model_generation_historical_binding_claimed=False)
