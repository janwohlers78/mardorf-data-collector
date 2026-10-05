"""Bounded causal reads of pinned public projections with private admission."""
from collections import OrderedDict
import time
from ..wp13.core_v1 import canonical, digest as content_id, strict_json, utc
from ..storage.objects import ObjectRef
from .assets import ROOT
from .prepared import policy, read_manifest, read_records, formatter
from .queue import ADMISSION

def group_key(field):
    """Logical entity excludes capture/job/revision and member index, not geometry."""
    source = field["source"]
    return content_id(
        {
            "domain": field["domain"],
            "kind": field["record_kind"],
            "provider": source["provider_binding_id"],
            "model": source["model_id_native"],
            "product": source["product_id_native"],
            "parameter": source["native_parameter"],
            "level": field["level"],
            "spatial": field["spatial_support"],
            "unit": field["unit_native"],
            "run": field["time"]["run_time_utc"],
            "measured": field["time"]["measured_at_utc"],
            "start": field["time"]["valid_start_utc"],
            "end": field["time"]["valid_end_utc"],
            "operator": field["time"]["operator"],
            "closure": field["time"]["closure"],
            "ensemble": field["member"]["ensemble_set_id"] is not None,
        }
    )

def value_signature(fields):
    return canonical(
        sorted(
            [
                {
                    "member": {
                        k: v for k, v in f["member"].items() if k != "ensemble_set_id"
                    },
                    "value": f["value_native"],
                    "qc": f["qc_native"],
                    "binding": f["binding_id"],
                    "extensions": {
                        k: v
                        for k, v in f["extensions"].items()
                        if k != "wp12:native-metadata-proof:v1"
                    },
                }
                for f in fields
            ],
            key=lambda x: canonical(x),
        )
    )


class VerifiedReader:
    def __init__(self, backend, *, expected_processor, root=ROOT):
        self.backend, self.expected_processor, self.root = backend, expected_processor, root
        self.cache = OrderedDict()
        self.cache_bytes = 0
        self.hits = 0

    def get_bytes(self, ref):
        key = (ref.key, ref.sha256, ref.bytes)
        if key in self.cache:
            self.hits += 1
            self.cache.move_to_end(key)
            return self.cache[key]
        body = self.backend.get_bytes(ref)
        maximum = policy(self.root)['cache_max_bytes']
        if len(body) <= maximum:
            while self.cache and self.cache_bytes + len(body) > maximum:
                _, old = self.cache.popitem(last=False)
                self.cache_bytes -= len(old)
            self.cache[key] = body
            self.cache_bytes += len(body)
        return body

    def read_weather(self, entries, *, profile_id, site_id, quantity_ids, start_utc,
                     end_utc, as_of_utc, view, configuration_sha256,
                     context=None, revision_mode='latest_visible', record_kind='observation'):
        started = time.monotonic()
        limits = policy(self.root)['query_limits']
        begin, end, cutoff = map(utc, (start_utc, end_utc, as_of_utc))
        if (not begin < end or (end-begin).total_seconds() > limits['max_days']*86400
                or view not in ('private_operational', 'public_reconstruction')
                or revision_mode not in ('latest_visible','all_visible')
                or context not in (None, 'prospective', 'historical') or record_kind != 'observation'):
            raise ValueError('Explicit bounded station query required')
        fmt = formatter(configuration_sha256, self.root)
        if (profile_id not in {r['id'] for r in fmt.registry['profiles']}
                or site_id not in {r['id'] for r in fmt.registry['sites']}
                or not quantity_ids or len(quantity_ids) != len(set(quantity_ids))
                or not set(quantity_ids) <= {r['id'] for r in fmt.registry['quantities']}):
            raise ValueError('Unregistered station query dimensions')
        selected = {}
        for identity, entry in entries.items():
            scope = entry['scope']
            if (scope['site_id'] == site_id and scope['profile_id'] == profile_id
                    and set(quantity_ids) & set(entry['quantities'])
                    and utc(scope['start_utc']) < end and begin <= utc(scope['end_utc'])
                    and (context is None or entry['context'] == context)):
                selected[identity] = entry
        if len(selected) > limits['max_partitions'] or sum(e['field_count'] for e in selected.values()) > limits['max_rows']:
            raise ValueError('Station query source/row budget exceeded; split request')
        groups = {}
        source_bytes = 0
        for identity, entry in sorted(selected.items()):
            doc = read_manifest(self, entry['manifest'], expected_processor=self.expected_processor, root=self.root)
            if (doc['publication_id'] != identity or doc['configuration_sha256'] != configuration_sha256
                    or entry['scope'] != doc['scope'] or entry['field_count'] != doc['field_count']
                    or entry['quantities'] != sorted(doc['quantity_counts'])
                    or entry['context'] != doc['envelope']['context']):
                raise ValueError('Admitted station catalog binding mismatch')
            receipt_ref = ObjectRef.parse(entry['admission'])
            if receipt_ref.key != ADMISSION+identity or receipt_ref.bytes > policy(self.root)['admission_max_bytes']:
                raise ValueError('Private admission reference mismatch')
            receipt = strict_json(self.get_bytes(receipt_ref))
            if (receipt['artifact_version'] != 'wp15-station-private-admission-v1'
                    or content_id({k:v for k,v in receipt.items() if k!='receipt_id'}) != receipt['receipt_id']
                    or receipt['manifest'] != entry['manifest'] or receipt['publication_id'] != identity
                    or receipt['processor_sha256'] != self.expected_processor
                    or receipt['context'] != doc['envelope']['context']
                    or receipt['native_fields_sha256'] != doc['native_fields_sha256']
                    or receipt['public_verified_at_utc'] != doc['public_verified_at_utc']
                    or utc(receipt['private_received_at_utc']) < utc(doc['public_verified_at_utc'])):
                raise ValueError('Private admission proof mismatch')
            captured = utc(doc['envelope']['capture']['retrieved_at_utc'])
            visible = max(captured, utc(receipt['private_received_at_utc'])) if view == 'private_operational' else captured
            if visible > cutoff:
                continue
            source_bytes += doc['native']['bytes'] + doc['projections']['bytes'] + sum(p['ref']['bytes'] for p in doc['partitions'])
            if source_bytes > limits['max_source_bytes']:
                raise ValueError('Station query byte budget exceeded')
            for record in read_records(self, doc):
                field = record['native']
                if (field['domain']['site_id'] != site_id or field['domain']['profile_id'] != profile_id
                        or field['domain']['quantity_id'] not in quantity_ids
                        or not begin <= utc(field['time']['valid_end_utc']) < end):
                    continue
                group = group_key(field)
                row = {'record':record, 'envelope_id':doc['envelope']['envelope_id'], 'receipt':receipt}
                groups.setdefault(group, []).append((captured, field['source']['source_revision'], identity, row))
            if time.monotonic()-started > limits['max_runtime_seconds']:
                raise ValueError('Station query runtime budget exceeded')
        rows = []
        for group in sorted(groups):
            versions = sorted(groups[group], key=lambda v:v[:3])
            for clock in {v[0] for v in versions}:
                if len({value_signature([v[3]['record']['native']]) for v in versions if v[0]==clock}) > 1:
                    raise ValueError('Ambiguous simultaneous station revisions')
            chosen = versions if revision_mode == 'all_visible' else versions[-1:]
            rows.extend(v[3] for v in chosen)
        return {'rows':rows, 'view':view, 'as_of_utc':as_of_utc,
            'validation_scope':'pinned_public_source_validation_and_hash_verified_projections',
            'metrics':{'source_transfers':len(selected), 'source_bytes_bound':source_bytes,
                'normalizations':0, 'original_reads':0, 'cache_hits':self.hits,
                'elapsed_seconds':time.monotonic()-started}}
