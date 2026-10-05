"""Successor semantic guard for immutable WP12 registry wire format V1."""
import re
from .domain_v1 import validate_domain, _utc, DomainErrorV1


def exact_utc(value):
    result=_utc(value)
    match=re.search(r'\.(\d+)(?:Z|[+-]\d\d:\d\d)$',value)
    if match and len(match.group(1))>6 and any(x!='0' for x in match.group(1)[6:]):
        raise DomainErrorV1('nonzero submicrosecond UTC precision loss')
    return result


def validate_domain_v2(value):
    tables=validate_domain(value)
    for collection in tables.values():
        for item in collection.values():
            exact_utc(item['valid_from_utc'])
            if item['valid_until_utc']:exact_utc(item['valid_until_utc'])
    def chain(items):
        lower=max(_utc(x['valid_from_utc']) for x in items)
        uppers=[_utc(x['valid_until_utc']) for x in items if x['valid_until_utc']]
        if uppers and lower>=min(uppers):
            raise DomainErrorV1('linked identities have disjoint validity windows')
    for station in value['stations']:
        chain([station,tables['sites'][station['site_id']],tables['providers'][station['provider_id']]])
    for sensor in value['sensors']:
        chain([sensor,tables['stations'][sensor['station_id']]])
    for binding in value['observation_bindings']:
        chain([binding,tables['sensors'][binding['sensor_id']],tables['quantities'][binding['quantity_id']]])
    for target in value['targets']:
        binding=tables['observation_bindings'][target['observation_binding_id']]
        sensor=tables['sensors'][binding['sensor_id']]
        station=tables['stations'][sensor['station_id']]
        identities=[target,binding,sensor,station,tables['sites'][target['site_id']],tables['quantities'][target['quantity_id']],tables['providers'][station['provider_id']]]
        chain(identities)
        for profile in value['profiles']:
            if target['id'] in profile['target_ids']:chain(identities+[profile])
    return tables
