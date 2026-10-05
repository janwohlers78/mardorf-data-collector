"""Audit successor: strict wire parsing, bounded decode and spatial atomicity.

V1 closed schemas and SI formulae remain byte-for-byte unchanged. Validation
contract V2 is explicit; clients must negotiate it before new writer activation.
"""
from copy import deepcopy
import bz2
import gzip
import hashlib
import io
import json
from pathlib import PurePosixPath
import re

from .fields_v1 import WeatherFormatV1, FieldErrorV1, canonical, content_id, with_id
from .domain_v2 import validate_domain_v2, exact_utc


class FieldErrorV2(FieldErrorV1):
    pass


def strict_json(payload):
    def pairs(items):
        result={}
        for k,v in items:
            if k in result:raise FieldErrorV2('duplicate JSON key')
            result[k]=v
        return result
    def constant(value):raise FieldErrorV2('nonfinite JSON number')
    try:
        value=json.loads(payload.decode('utf-8') if isinstance(payload,bytes) else payload,object_pairs_hook=pairs,parse_constant=constant)
        canonical(value)
        return value
    except (UnicodeError,ValueError,TypeError) as exc:
        raise FieldErrorV2('strict UTF-8 finite JSON: '+str(exc)) from exc


def pointer(document, path):
    if not isinstance(path,str) or (path and not path.startswith('/')):
        raise FieldErrorV2('RFC6901 pointer required')
    node=document
    try:
        for part in path.split('/')[1:] if path else []:
            if re.search(r'~(?![01])',part):raise FieldErrorV2('invalid RFC6901 escape')
            part=part.replace('~1','/').replace('~0','~')
            if isinstance(node,list):
                if not re.fullmatch(r'0|[1-9][0-9]*',part):raise FieldErrorV2('invalid RFC6901 array index')
                node=node[int(part)]
            else:node=node[part]
        return node
    except FieldErrorV2:
        raise
    except (KeyError,IndexError,ValueError,TypeError) as exc:
        raise FieldErrorV2('RFC6901 pointer missing') from exc


def safe_path(value):
    if not isinstance(value,str) or not value or '\\' in value or ':' in value or '\x00' in value or value.startswith('/') or any(x in ('','.','..') for x in value.split('/')) or str(PurePosixPath(value))!=value:
        raise FieldErrorV2('portable relative POSIX path required')


def bounded_decode(item,payload,limit):
    try:
        if item['compression']=='none':decoded=payload
        else:
            reader=gzip.GzipFile(fileobj=io.BytesIO(payload)) if item['compression']=='gzip' else bz2.BZ2File(io.BytesIO(payload))
            with reader:decoded=reader.read(limit+1)
        if len(decoded)>limit:raise FieldErrorV2('decoded byte budget exceeded')
        return decoded
    except (OSError,EOFError,ValueError) as exc:
        raise FieldErrorV2('bounded raw decode failed: '+str(exc)) from exc


class WeatherFormatV2(WeatherFormatV1):
    def __init__(self,root=None,*,fixture_registry_path=None):
        if root is None:super().__init__()
        else:super().__init__(root)
        self.contract=self.read('config/dev03_wp12_format_contract_v2.json')
        for item in self.contract['audit_predecessors']:
            if hashlib.sha256((self.root/item['path']).read_bytes()).hexdigest()!=item['sha256']:
                raise FieldErrorV2('audit predecessor checksum mismatch')
        if fixture_registry_path is not None:
            if fixture_registry_path not in self.contract['fixture_registry_allowlist']:
                raise FieldErrorV2('unregistered fixture domain')
            from .domain_v1 import DomainRegistryV1, validate_successor
            previous=self.registry
            self.registry=self.read(fixture_registry_path)
            validate_successor(previous,self.registry)
            self.domain=DomainRegistryV1(self.registry)
        validate_domain_v2(self.registry)
        self.limits=self.read('config/dev03_wp12_lifecycle_contract_v1.json')['limits']

    def validate_field(self,field):
        super().validate_field(field)
        if field['record_kind']=='observation':
            from urllib.parse import urlparse
            station=next(x for x in self.registry['stations'] if x['id']==field['domain']['station_id'])
            provider=next(x for x in self.registry['providers'] if x['id']==field['source']['provider_binding_id'])
            binding=self.weatherlink[field['binding_id']]
            actual=urlparse(field['source']['source_url']);base=urlparse(provider['endpoint'])
            expected=base.path.rstrip('/')+'/'+binding['endpoint_kind']+'/'+station['provider_station_id']
            if actual.scheme!=base.scheme or actual.hostname!=base.hostname or actual.port not in (None,443) or actual.username or actual.password or actual.path.rstrip('/')!=expected:
                raise FieldErrorV2('observation source station/endpoint contradiction')
        t=field['time']
        for k,v in t.items():
            if k.endswith('_utc') and v is not None:exact_utc(v)
        if t['operator']=='unknown' and t['closure']!='unknown':
            raise FieldErrorV2('unknown operator requires unknown closure')
        if field['domain']['target_id'] and field['record_kind']=='observation':
            target=next(x for x in self.registry['targets'] if x['id']==field['domain']['target_id'])
            binding=next(x for x in self.registry['observation_bindings'] if x['id']==target['observation_binding_id'])
            if binding['sensor_id']!=field['domain']['sensor_id'] or binding['native_field']!=field['source']['native_parameter']:
                raise FieldErrorV2('observation target sensor/native field mismatch')
        return deepcopy(field)

    def normalize(self,field):
        out=super().normalize(field)
        if field['unit_evidence']=='unknown' and out['status']=='normalized':
            out.update(status='semantic_open',value_canonical=None,unit_canonical=None,physical_kind=None,matching_status='open',reasons=['native_unit_evidence_unknown'],method_version='wp12-unproven-unit-v2')
        # A field row assertion alone is not proof of native model metadata.
        # Scalar SI conversion remains useful; matching stays open until bound proof.
        if field['record_kind']=='forecast' and not field.get('extensions',{}).get('wp12:native-metadata-proof:v1') and out['matching_status']=='eligible':
            out.update(matching_status='open',reasons=out['reasons']+['native_metadata_proof_missing'])
        out=with_id(out,'projection_id');self.check('canonical_field',out,'projection_id')
        return out

    def validate_transfer(self,envelope,fields,raw_bytes,fragment_bytes):
        self.check('envelope',envelope,'envelope_id')
        for k,v in envelope['capture'].items():
            if k.endswith('_utc'):exact_utc(v)
        l=self.limits
        if len(fields)>l['max_member_fields'] or len(envelope['raw_objects'])>l['max_raw_objects'] or len(envelope['field_fragments'])>l['max_fragments'] or sum(map(len,raw_bytes.values()))>l['max_payload_bytes'] or sum(map(len,fragment_bytes.values()))>l['max_fragment_bytes']:
            raise FieldErrorV2('transfer byte/row/object budget exceeded')
        if set(raw_bytes)!={x['object_id'] for x in envelope['raw_objects']} or set(fragment_bytes)!={x['path'] for x in envelope['field_fragments']}:
            raise FieldErrorV2('undeclared or missing transfer objects')
        docs={};total=0
        for item in envelope['raw_objects']:
            safe_path(item['path'])
            payload=raw_bytes[item['object_id']]
            if not isinstance(payload,bytes) or len(payload)!=item['transport_bytes'] or hashlib.sha256(payload).hexdigest()!=item['transport_sha256']:
                raise FieldErrorV2('raw transport integrity mismatch')
            decoded=bounded_decode(item,payload,l['max_decoded_bytes'])
            total+=len(decoded)
            if total>l['max_total_decoded_bytes']:raise FieldErrorV2('total decoded byte budget exceeded')
            if hashlib.sha256(decoded).hexdigest()!=item['decoded_sha256']:raise FieldErrorV2('decoded integrity mismatch')
            if item['media_type']=='application/json':docs[item['object_id']]=strict_json(decoded)
        for item in envelope['field_fragments']:
            safe_path(item['path'])
            payload=fragment_bytes[item['path']]
            if not payload.endswith(b'\n'):raise FieldErrorV2('JSONL final LF required')
            for line in payload.splitlines():strict_json(line)
        groups={}
        tables={k:{x['id']:x for x in self.registry[k]} for k in ('profiles','sites')}
        for f in fields:
            self.validate_field(f)
            if envelope['intended_use']=='development' and (tables['profiles'][f['domain']['profile_id']]['usage']!='development' or tables['sites'][f['domain']['site_id']]['usage']!='development'):
                raise FieldErrorV2('fixture/legacy profile cannot enter development transfer')
            source=f['source'];doc=docs.get(source['native_metadata_object_id'] or source['raw_object_id'])
            if doc is not None:pointer(doc,source['raw_record_pointer'])
            proof=f.get('extensions',{}).get('wp12:native-metadata-proof:v1')
            if proof:
                if set(proof)!={'raw_object_id','pointer','sha256'} or proof['raw_object_id'] not in docs:
                    raise FieldErrorV2('native metadata proof structure missing')
                body=raw_bytes[proof['raw_object_id']]
                if hashlib.sha256(body).hexdigest()!=proof['sha256']:raise FieldErrorV2('native metadata proof checksum mismatch')
                native=pointer(docs[proof['raw_object_id']],proof['pointer'])
                expected={k:f[k] for k in ('level','spatial_support','member','unit_native')}
                expected['time']={k:v for k,v in f['time'].items() if k not in ('first_seen_at_utc','first_seen_scope','private_first_seen_at_utc')}
                if canonical(native)!=canonical(expected):raise FieldErrorV2('native metadata time/place/member/unit contradiction')
            member=f['member']
            if member['ensemble_set_id']:
                key=(member['ensemble_set_id'],source['product_id_native'],f['domain']['site_id'],f['domain']['quantity_id'],content_id(f['level']),content_id(f['time']))
                support=content_id({'spatial_support':f['spatial_support'],'unit_native':f['unit_native'],'binding_id':f['binding_id']})
                groups.setdefault(key,set()).add(support)
        if any(len(v)!=1 for v in groups.values()):raise FieldErrorV2('ensemble mixed spatial support/unit/binding')
        return super().validate_transfer(envelope,fields,raw_bytes,fragment_bytes)
