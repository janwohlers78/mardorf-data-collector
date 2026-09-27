"""Provider-native CAPE identity and hazard-selection contract for Phase-2 audit B2."""
from __future__ import annotations

import json
import math
from pathlib import Path

CONTRACT_PATH=Path(__file__).resolve().parents[1]/"config"/"cape_native_identity_contract_v1.json"
CONTRACT=json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
CONTRACT_VERSION=CONTRACT["method_version"]
THRESHOLD_JKG=float(CONTRACT["threshold_jkg"])


def finite(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(float(value))


def _token(value):
    return None if value is None else str(value).strip()


def _lower(value):
    x=_token(value)
    return None if x is None else x.lower()


def _unit_ok(value):
    token=_lower(value)
    if token is None:
        return False
    token=token.replace(" ","").replace("**","^")
    return token in {"j/kg","jkg-1","jkg^-1"}


def _matches(value, allowed, *, lower=False):
    if not allowed:
        return True
    value=_lower(value) if lower else _token(value)
    expected={str(x).strip().lower() if lower else str(x).strip() for x in allowed}
    return value in expected


def is_cape_like(parameter,item):
    parameter=_lower(parameter)
    if parameter in {"cape","mucape","cape_ml"}:
        return True
    if not isinstance(item,dict):
        return False
    return _lower(item.get("semantic_id"))=="cape" or _lower(item.get("shortName")) in {"cape","mucape","cape_ml"}


def identify_item(model,parameter,item):
    """Return one exact contract identity or an explicit ambiguity reason."""
    if not isinstance(item,dict) or not is_cape_like(parameter,item):
        return {"status":"not_cape"}
    provider=(CONTRACT.get("providers") or {}).get(model)
    if not provider or provider.get("source")!="values":
        return {"status":"ambiguous","reason":"model_or_source_not_contracted"}
    if not finite(item.get("value")) or float(item["value"])<0:
        return {"status":"ambiguous","reason":"cape_value_not_finite_nonnegative"}
    if not _unit_ok(item.get("units")):
        return {"status":"ambiguous","reason":"cape_unit_missing_or_uncontracted"}
    if _lower(item.get("stepType")) not in {"instant","instantaneous"}:
        return {"status":"ambiguous","reason":"cape_step_type_not_instantaneous"}
    matches=[]
    for spec in provider.get("identities") or []:
        if not _matches(parameter,spec.get("parameters"),lower=True):
            continue
        if not _matches(item.get("shortName"),spec.get("short_names"),lower=True):
            continue
        if not _matches(item.get("paramId"),spec.get("param_ids")):
            continue
        if not _matches(item.get("typeOfLevel"),spec.get("type_of_levels")):
            continue
        if not _matches(item.get("level"),spec.get("levels")):
            continue
        if not _matches(item.get("provider_product"),spec.get("provider_products"),lower=True):
            continue
        matches.append(spec)
    if len(matches)!=1:
        return {
            "status":"ambiguous",
            "reason":"no_exact_native_identity_match" if not matches else "multiple_native_identity_matches",
            "parameter_native":str(parameter),
            "shortName":item.get("shortName"),
            "paramId":item.get("paramId"),
            "typeOfLevel":item.get("typeOfLevel"),
            "level":item.get("level"),
            "provider_product":item.get("provider_product"),
        }
    spec=matches[0]
    declared=item.get("cape_native_identity_id")
    if declared is not None and str(declared)!=spec["identity_id"]:
        return {
            "status":"ambiguous","reason":"producer_declared_identity_mismatch",
            "declared_identity_id":str(declared),"contract_identity_id":spec["identity_id"],
        }
    declared_contract=item.get("cape_identity_contract_version")
    if declared_contract is not None and str(declared_contract)!=CONTRACT_VERSION:
        return {
            "status":"ambiguous","reason":"producer_cape_identity_contract_mismatch",
            "declared_contract_version":str(declared_contract),
            "expected_contract_version":CONTRACT_VERSION,
        }
    return {
        "status":"identified",
        "contract_version":CONTRACT_VERSION,
        "identity_id":spec["identity_id"],
        "model":model,
        "parameter_native":str(parameter),
        "value_jkg":float(item["value"]),
        "shortName":item.get("shortName"),
        "paramId":item.get("paramId"),
        "typeOfLevel":item.get("typeOfLevel"),
        "level":item.get("level"),
        "provider_product":item.get("provider_product"),
        "unit_native":item.get("units"),
        "step_type_native":item.get("stepType"),
        "step_range_native":item.get("stepRange"),
        "member_id":None,
    }


def _member_evidence(model,record):
    provider=(CONTRACT.get("providers") or {}).get(model) or {}
    if provider.get("source")!="members":
        return [],[]
    specs=provider.get("identities") or []
    if len(specs)!=1:
        return [],[{"model":model,"reason":"member_identity_contract_ambiguous"}]
    spec=specs[0]
    members=record.get("members")
    if not isinstance(members,list):
        return [],[{"model":model,"reason":"member_cape_source_missing"}]
    expected=int(provider.get("expected_member_count") or 0)
    candidates=[];problems=[]
    seen=set()
    for member in members:
        if not isinstance(member,dict):
            problems.append({"model":model,"reason":"member_entry_invalid"})
            continue
        member_id=member.get("member")
        if member_id in seen:
            problems.append({"model":model,"member_id":member_id,"reason":"duplicate_member_id"})
            continue
        seen.add(member_id)
        value=member.get("cape")
        if not finite(value) or float(value)<0:
            problems.append({"model":model,"member_id":member_id,"reason":"member_cape_missing_or_invalid"})
            continue
        candidates.append({
            "status":"identified",
            "contract_version":CONTRACT_VERSION,
            "identity_id":spec["identity_id"],
            "model":model,
            "parameter_native":"cape",
            "value_jkg":float(value),
            "shortName":None,
            "paramId":None,
            "typeOfLevel":None,
            "level":None,
            "provider_product":(spec.get("provider_products") or [None])[0],
            "unit_native":None,
            "step_type_native":"instantaneous",
            "step_range_native":None,
            "member_id":member_id,
        })
    if expected and len(seen)!=expected:
        problems.append({
            "model":model,"reason":"member_set_incomplete",
            "expected_member_count":expected,"observed_member_count":len(seen),
        })
    return candidates,problems


def record_cape_evidence(model,record):
    provider=(CONTRACT.get("providers") or {}).get(model)
    if not provider:
        return {"model":model,"candidates":[],"problems":[],"capable":False}
    if provider.get("source")=="members":
        candidates,problems=_member_evidence(model,record)
        return {"model":model,"candidates":candidates,"problems":problems,"capable":True}
    values=record.get("values") if isinstance(record.get("values"),dict) else {}
    candidates=[];problems=[]
    for parameter,raw in values.items():
        items=raw if isinstance(raw,list) else [raw] if isinstance(raw,dict) else []
        for item in items:
            if not is_cape_like(parameter,item):
                continue
            result=identify_item(model,parameter,item)
            if result.get("status")=="identified":
                candidates.append(result)
            else:
                problems.append({"model":model,**result})
    # Same native identity may be duplicated only if its numeric value is identical.
    grouped={}
    for candidate in candidates:
        key=(candidate["identity_id"],candidate.get("member_id"))
        grouped.setdefault(key,[]).append(candidate)
    deduped=[]
    for key,items in grouped.items():
        values_seen={round(float(x["value_jkg"]),9) for x in items}
        if len(values_seen)>1:
            problems.append({
                "model":model,"identity_id":key[0],"member_id":key[1],
                "reason":"duplicate_identity_value_conflict",
                "values_jkg":sorted(values_seen),
            })
        else:
            deduped.append(sorted(items,key=lambda x:(
                str(x.get("provider_product") or ""),str(x.get("typeOfLevel") or ""),
                str(x.get("level") or "")
            ))[0])
    return {"model":model,"candidates":deduped,"problems":problems,"capable":True}


def evaluate_records(records,threshold_jkg=None):
    """Evaluate CAPE hazard without depending on native-list ordering."""
    threshold=THRESHOLD_JKG if threshold_jkg is None else float(threshold_jkg)
    candidates=[];problems=[];missing_models=[]
    for model,record in records:
        evidence=record_cape_evidence(model,record)
        if not evidence["capable"]:
            continue
        candidates.extend(evidence["candidates"])
        problems.extend(evidence["problems"])
        if not evidence["candidates"]:
            missing_models.append(model)
    candidates=sorted(candidates,key=lambda x:(
        x["identity_id"],str(x.get("member_id")),str(x.get("provider_product") or ""),float(x["value_jkg"])
    ))
    selected=max(
        candidates,
        key=lambda x:(float(x["value_jkg"]),x["identity_id"],str(x.get("member_id"))),
        default=None,
    )
    positive=any(float(x["value_jkg"])>=threshold for x in candidates)
    if positive:
        signal=True;status="positive"
    elif problems:
        signal=None;status="ambiguous"
    elif missing_models or not candidates:
        signal=None;status="missing"
    else:
        signal=False;status="negative"
    return {
        "contract_version":CONTRACT_VERSION,
        "threshold_jkg":threshold,
        "signal":signal,
        "status":status,
        "max_jkg":None if selected is None else float(selected["value_jkg"]),
        "selected_identity":selected,
        "candidate_count":len(candidates),
        "source_models":sorted({x["model"] for x in candidates}),
        "missing_models":sorted(set(missing_models)),
        "problems":sorted(problems,key=lambda x:json.dumps(x,sort_keys=True,default=str)),
        "candidates":candidates,
    }
