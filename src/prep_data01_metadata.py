"""Shared read-only PREP-DATA-01 diagnostics; no native metadata inference."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path

VERSION = 'prep-data01-readiness-v1'
CONTRACT_PATH = Path(__file__).resolve().parents[1] / 'config/prep_data01_readiness_contract_v1.json'
ALIASES = {
    'parameter_native': ('parameter_native', 'shortName'),
    'unit_native': ('unit_native', 'units', 'unit'),
    'type_of_level_native': ('type_of_level_native', 'typeOfLevel'),
    'level_native': ('level_native', 'level'),
    'step_type_native': ('step_type_native', 'stepType'),
}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def contract():
    policy = json.loads(CONTRACT_PATH.read_bytes())
    if policy.get('artifact_version') != VERSION or policy.get('schema_version') != 1:
        raise ValueError('unsupported PREP-DATA-01 contract')
    if policy.get('calibration_authorized') is not False:
        raise ValueError('PREP-DATA-01 cannot authorize calibration')
    return policy


def dimension_report(field, *, policy=None):
    policy = contract() if policy is None else policy
    values, states = {}, {}
    for dimension in policy['required_native_dimensions']:
        supplied = [field[k] for k in ALIASES[dimension] if k in field and field[k] is not None]
        # Differing types/spellings are conflicting evidence, even if numerically equal.
        conflicting = len({json.dumps(v, sort_keys=True, allow_nan=False) for v in supplied}) > 1
        value = supplied[0] if supplied else None
        values[dimension] = deepcopy(value)
        if conflicting:
            state = 'conflicting'
        elif value is None or (type(value) is str and not value.strip()):
            state = 'missing'
        elif type(value) is str and value in policy['unknown_spellings']:
            state = 'unknown'
        elif dimension == 'level_native':
            try:
                number = float(value) if type(value) in (int, float, str) else float('nan')
                state = 'present' if math.isfinite(number) else 'invalid'
            except (ValueError, OverflowError):
                state = 'invalid'
        elif type(value) is not str:
            state = 'invalid'
        elif dimension == 'type_of_level_native' and value not in policy['known_level_types']:
            state = 'unrecognized'
        elif dimension == 'step_type_native' and value not in policy['known_step_types']:
            state = 'unrecognized'
        else:
            state = 'present'
        states[dimension] = state
    return {'native_dimensions': values, 'dimension_states': states,
            'schema_present': all(v not in ('missing', 'conflicting', 'invalid') for v in states.values()),
            'scientifically_known': all(v == 'present' for v in states.values()),
            'evidence_sha256': digest(field)}


def acquisition_inventory(payload):
    """Inspect stored native fields and explicit ensemble product specifications.

    Specification grain is one field/member specification, not one measurement.
    Paths and the exact payload hash make every diagnosis inspectable upstream.
    """
    policy = contract()
    records = []

    def append(field, path, context, grain, member=None):
        report = dimension_report(field, policy=policy)
        records.append({'path': path, 'grain': grain, 'model': context.get('model'),
                        'ensemble_system_id': context.get('ensemble_system_id'),
                        'member_id': member if member is not None else context.get('member_id'),
                        'semantic_id': field.get('semantic_id'),
                        'provider_product': field.get('field_provider_product') or context.get('provider_product'),
                        'run_time_utc': context.get('run_time_utc'),
                        'valid_time_utc': context.get('valid_time_utc'),
                        'product_metadata': deepcopy(field) if grain == 'product_field_member_specification' else None,
                        **report})

    def walk(node, path='', context=None):
        context = dict(context or {})
        if type(node) is dict:
            for k in ('model', 'ensemble_system_id', 'member_id', 'provider_product', 'run_time_utc', 'valid_time_utc'):
                if node.get(k) is not None:
                    context[k] = node[k]
            specs = node.get('field_specs')
            if type(specs) is dict:
                for key, spec in sorted(specs.items()):
                    if type(spec) is not dict:
                        raise ValueError('field specification must be an object')
                    columns = (node.get('columns') or {}).get(key, {})
                    # Actual persisted member columns, not an inferred expected-member list.
                    members = sorted(columns, key=str) if type(columns) is dict else []
                    for member in members or [None]:
                        append(spec, path + '/field_specs/' + key, context,
                               'product_field_member_specification', member)
            for key, item in node.items():
                if key == 'field_specs':
                    continue
                if key == 'values' and type(item) is dict:
                    for parameter, fields in sorted(item.items()):
                        if type(fields) is not list:
                            raise ValueError('native values must be field lists')
                        for index, field in enumerate(fields):
                            if type(field) is not dict:
                                raise ValueError('native field must be an object')
                            append(field, f'{path}/values/{parameter}/{index}', context, 'native_field')
                elif key == 'fields' and type(item) is list:
                    for index, field in enumerate(item):
                        if type(field) is dict and ('value_native' in field or 'parameter_native' in field):
                            append(field, f'{path}/fields/{index}', context, 'native_field')
                elif key == 'models' and type(item) is dict:
                    for model, rows in sorted(item.items()):
                        walk(rows, path + '/models/' + model, dict(context, model=model))
                elif type(item) in (dict, list):
                    walk(item, path + '/' + key, context)
        elif type(node) is list:
            for index, item in enumerate(node):
                walk(item, path + '/' + str(index), context)

    walk(payload)
    counts = Counter((dimension, state) for row in records
                     for dimension, state in row['dimension_states'].items())
    return {'schema_version': 1, 'method_version': VERSION, 'policy_sha256': digest(policy),
            'payload_sha256': digest(payload), 'scope': 'persisted native fields/product specifications; not samples',
            'records': records, 'record_count': len(records),
            'dimension_counts': {d: dict(sorted((s, n) for (k, s), n in counts.items() if k == d))
                                 for d in policy['required_native_dimensions']},
            'calibration_authorized': False}


def attach_metadata(report, raw):
    if report.get('input_payload_sha256') != hashlib.sha256(raw).hexdigest():
        raise ValueError('metadata report must bind the exact audited payload')
    inventory = acquisition_inventory(json.loads(raw))
    inventory['input_payload_raw_sha256'] = hashlib.sha256(raw).hexdigest()
    return {**report, 'prep_data01_metadata': inventory}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--integrity-json', type=Path, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    report = json.loads(args.integrity_json.read_bytes())
    report = attach_metadata(report, raw)
    inventory = report['prep_data01_metadata']
    args.integrity_json.write_text(json.dumps(report, sort_keys=True, allow_nan=False) + '\n')
    print(json.dumps({'metadata_records': inventory['record_count'],
                      'dimension_counts': inventory['dimension_counts'], 'calibration_authorized': False}))


if __name__ == '__main__':
    main()
