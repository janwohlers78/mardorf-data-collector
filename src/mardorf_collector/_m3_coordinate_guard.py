"""Bound metadata/coordinate reads before transfer and coordinate decoding.

Sits inside the unchanged weather-range guard. Only immutable snapshot metadata
can admit coordinate chunks. Changed coordinate codecs/shapes fail before load.
"""
import hashlib
import json
import math

COORDINATES={'init_time','lead_time','valid_time','ensemble_member','latitude','longitude',
             'expected_forecast_length','ingested_forecast_length','spatial_ref'}
ENCODED_MAX=4*1024**2
DECODED_TOTAL_MAX=128*1024**2
TYPE_BYTES={'int8':1,'uint8':1,'int16':2,'uint16':2,'int32':4,'uint32':4,
            'int64':8,'uint64':8,'float32':4,'float64':8,'bool':1}


def coordinate_bounded_store(store,WrapperStore,RangeByteRequest):
    class CoordinateGuard(WrapperStore):
        def __init__(self,source):
            super().__init__(source); self.approved={}; self.metadata_hashes={}
        def admit(self,name,metadata):
            if name not in COORDINATES: return
            dtype=metadata.get('data_type'); grid=metadata.get('chunk_grid',{})
            chunks=grid.get('configuration',{}).get('chunk_shape')
            shape=metadata.get('shape'); codecs=metadata.get('codecs',[])
            if (metadata.get('zarr_format')!=3 or metadata.get('node_type')!='array'
                    or dtype not in TYPE_BYTES or grid.get('name')!='regular'
                    or not isinstance(chunks,list) or not isinstance(shape,list) or len(chunks)!=len(shape)
                    or any(type(v) is not int or v<=0 for v in chunks)
                    or any(type(v) is not int or v<0 for v in shape)
                    or metadata.get('storage_transformers',[]) or not codecs
                    or codecs[0]!={'name':'bytes','configuration':{'endian':'little'}}
                    or len(codecs)>2):
                raise ValueError('coordinate_native_layout_changed_before_decode')
            if len(codecs)==2:
                c=codecs[1]; config=c.get('configuration',{})
                if (c.get('name')!='blosc' or config.get('cname')!='zstd'
                        or config.get('typesize')!=TYPE_BYTES[dtype]
                        or type(config.get('clevel')) is not int or not 0<=config['clevel']<=9
                        or config.get('shuffle') not in ('shuffle','bitshuffle','noshuffle')
                        or config.get('blocksize')!=0):
                    raise ValueError('coordinate_codec_changed_before_decode')
            chunk_decoded=math.prod(chunks)*TYPE_BYTES[dtype]
            full_array_decoded=math.prod(shape)*TYPE_BYTES[dtype]
            # Xarray may allocate full coordinate indexes eagerly. Account for
            # both output and a decoded/cache copy before permitting any chunk.
            decoded=2*max(chunk_decoded,full_array_decoded)
            pin=hashlib.sha256(json.dumps(metadata,sort_keys=True,separators=(',',':')).encode()).hexdigest()
            if name in self.metadata_hashes and self.metadata_hashes[name]!=pin:
                raise ValueError('immutable_coordinate_metadata_changed')
            next_sizes=dict(self.approved,**{name:decoded})
            if sum(next_sizes.values())>DECODED_TOTAL_MAX:
                raise ValueError('coordinate_decoded_peak_exceeds128MiB_before_decode')
            self.approved[name]=decoded; self.metadata_hashes[name]=pin
        def metadata(self,key,value):
            if value is None: return
            raw=value.to_bytes() if hasattr(value,'to_bytes') else bytes(value)
            if len(raw)>ENCODED_MAX: raise ValueError('metadata_payload_exceeds_bound_before_parse')
            doc=json.loads(raw)
            name=key.split('/',1)[0]
            if key.endswith('/zarr.json'): self.admit(name,doc)
            if key=='zarr.json':
                for child,metadata in doc.get('consolidated_metadata',{}).get('metadata',{}).items():
                    self.admit(child.removesuffix('/zarr.json'),metadata)
        async def get(self,key,prototype,byte_range=None):
            key=str(key); first=key.split('/',1)[0]
            metadata=(key in ('zarr.json','.zmetadata','.zattrs','.zgroup') or key.endswith(('/zarr.json','/.zarray','/.zattrs','/.zgroup')))
            coordinate=first in COORDINATES and not metadata
            if not metadata and not coordinate:
                return await self._store.get(key,prototype=prototype,byte_range=byte_range)
            if coordinate and first not in self.approved:
                raise ValueError('coordinate_payload_requested_before_metadata_admission')
            if byte_range is None:
                byte_range=RangeByteRequest(0,ENCODED_MAX+1)
            elif type(byte_range).__name__=='RangeByteRequest':
                if (type(byte_range.start) is not int or type(byte_range.end) is not int
                        or not 0<=byte_range.start<byte_range.end or byte_range.end-byte_range.start>ENCODED_MAX):
                    raise ValueError('coordinate_or_metadata_range_exceeds_bound')
            elif type(byte_range).__name__=='SuffixByteRequest':
                if not 0<byte_range.suffix<=ENCODED_MAX: raise ValueError('coordinate_suffix_exceeds_bound')
            else: raise ValueError('coordinate_or_metadata_request_not_bounded')
            value=await self._store.get(key,prototype=prototype,byte_range=byte_range)
            if value is not None and len(value)>ENCODED_MAX:
                raise ValueError('metadata_coordinate_payload_exceeds_bound_before_decode')
            if metadata and value is not None and (key=='zarr.json' or key.endswith('/zarr.json')):
                self.metadata(key,value)
            return value
    result=CoordinateGuard(store)
    if not result.read_only: raise ValueError('readonly_coordinate_source_required')
    return result
