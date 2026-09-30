"""ECMWF IFS Open Data mapping for Relevant Meteorology Registry v1.

The mapping is based on the live Phase-2F-2 product probe. Pressure-level
relative humidity (shortName r) is deliberately not mapped to 2 m relative
humidity. Native CIN is not exposed by the probed IFS Open Data oper/fc
product. Unsupported Registry semantics are therefore emitted explicitly as
availability declarations rather than synthesized values.
"""
from __future__ import annotations

import json
import subprocess

from mardorf_collector.providers.grib_identity import _step_end_hours
import mardorf_collector.contracts.cape_native_identity as cape_identity

REGISTRY_VERSION = "relevant-meteorology-v1"
METHOD_VERSION = "phase2f2-ecmwf-registry-v1-routine-v1"
PRODUCT = "ifs_oper_fc_0p25"

PARAMS = (
    "10u", "10v", "10fg", "10fg3",
    "tp", "mucape",
    "2t", "2d", "sp", "msl", "tcc", "ssrd",
)

SEMANTICS = {
    "10u": "wind_u_10m",
    "10v": "wind_v_10m",
    "10fg": "wind_gust_10m",
    "10fg3": "wind_gust_10m",
    "10fg6": "wind_gust_10m",
    "2t": "air_temperature_2m",
    "2d": "dewpoint_temperature_2m",
    "sp": "surface_pressure",
    "msl": "mean_sea_level_pressure",
    "tcc": "total_cloud_cover",
    "ssrd": "surface_downward_shortwave",
    "tp": "total_precipitation",
    "mucape": "cape",
}

UNSUPPORTED = (
    {
        "semantic_id": "relative_humidity_2m",
        "parameter_native": "2r",
        "reason": (
            "IFS Open Data oper/fc exposes pressure-level r but no unambiguous "
            "native 2 m relative-humidity field; do not substitute pressure-level r"
        ),
    },
    {
        "semantic_id": "cin",
        "parameter_native": "cin",
        "reason": "IFS Open Data oper/fc live probe exposes no native CIN field",
    },
)

_METADATA_KEYS = (
    "shortName", "paramId", "typeOfLevel", "level", "stepType", "stepRange",
    "startStep", "endStep", "stepUnits", "units", "dataDate", "dataTime",
    "validityDate", "validityTime",
)


def message_metadata(path):
    """Return provider GRIB metadata in message order using ecCodes JSON."""
    proc = subprocess.run(
        ["grib_ls", "-j", "-p", ",".join(_METADATA_KEYS), str(path)],
        capture_output=True, text=True, check=True,
    )
    payload = json.loads(proc.stdout)
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise RuntimeError(f"no ECMWF GRIB metadata parsed for {path}")
    return messages


def _lead(step_range):
    value = _step_end_hours(step_range)
    if value is None or abs(value - round(value)) > 1e-9:
        return None
    return int(round(value))


def values_by_lead(nearest_rows, metadata, requested_leads, source_sha256=None):
    """Combine nearest-point values with full native metadata without normalization."""
    if len(nearest_rows) != len(metadata):
        raise RuntimeError(
            f"ECMWF metadata/nearest row mismatch metadata={len(metadata)} "
            f"nearest={len(nearest_rows)}"
        )
    out = {int(x): {} for x in requested_leads}
    for (nearest_name, nearest_step, value), raw in zip(nearest_rows, metadata):
        item = dict(raw)
        name = str(item.get("shortName") or nearest_name)
        meta_step = item.get("stepRange")
        if name != str(nearest_name):
            raise RuntimeError(
                f"ECMWF GRIB message order mismatch shortName metadata={name} "
                f"nearest={nearest_name}"
            )
        if meta_step is not None and str(meta_step) != str(nearest_step):
            raise RuntimeError(
                f"ECMWF GRIB message order mismatch stepRange metadata={meta_step} "
                f"nearest={nearest_step}"
            )
        lead = _lead(meta_step if meta_step is not None else nearest_step)
        if lead not in out:
            continue
        semantic = SEMANTICS.get(name)
        if semantic is None:
            raise RuntimeError(f"unmapped ECMWF requested field {name!r}")
        item.update({
            "semantic_id": semantic,
            "provider_product": PRODUCT,
            "value": float(value),
            "availability_status": "received",
            "availability_evidence_type": "ecmwf_open_data_grib_message",
        })
        if semantic=="cape":
            identity=cape_identity.identify_item("ECMWF-IFS",name,item)
            item["cape_identity_contract_version"]=cape_identity.CONTRACT_VERSION
            item["cape_native_identity_status"]=identity.get("status")
            if identity.get("status")=="identified":
                item["cape_native_identity_id"]=identity["identity_id"]
            else:
                item["cape_native_identity_reason"]=identity.get("reason")
        if source_sha256:
            item["source_sha256"] = str(source_sha256)
        out[lead].setdefault(name, []).append(item)
    return out


def unsupported_declarations():
    return [
        {
            "semantic_id": item["semantic_id"],
            "parameter_native": item["parameter_native"],
            "namespace": "availability",
            "field_provider_product": PRODUCT,
            "availability_status": "unsupported_by_provider_or_product",
            "availability_evidence_type": "phase2f2_live_product_probe",
            "reason": item["reason"],
        }
        for item in UNSUPPORTED
    ]


def acquisition_summary():
    return {
        "schema_version": 1,
        "method_version": METHOD_VERSION,
        "registry_version": REGISTRY_VERSION,
        "provider_product": PRODUCT,
        "requested_native_fields": list(PARAMS),
        "semantic_mapping": dict(SEMANTICS),
        "unsupported_registry_semantics": [
            {
                "semantic_id": x["semantic_id"],
                "parameter_native": x["parameter_native"],
                "reason": x["reason"],
            }
            for x in UNSUPPORTED
        ],
        "normalization_performed": False,
        "analysis_changed": False,
        "native_surface_relative_humidity_available": False,
        "native_cin_available": False,
        "pressure_level_r_must_not_map_to_relative_humidity_2m": True,
    }
