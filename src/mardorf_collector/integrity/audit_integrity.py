#!/usr/bin/env python3
from __future__ import annotations
import argparse,hashlib,json,math,os
from collections import Counter
from datetime import datetime,timedelta,timezone
from pathlib import Path

POLICY=Path("config/integrity_policy.json")
FAMILY={
    "ICON-D2":"DWD-ICON","ICON-D2-EPS":"DWD-ICON","ICON-EU":"DWD-ICON",
    "ECMWF-IFS":"ECMWF","GFS":"GFS","GEFS-control":"GFS",
}
POLICY_VERSION="collector-integrity-v1.6"

def dt(v):
    if not v:return None
    try:
        x=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
    except Exception:return None

def finite(v):
    return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)

def issue(code,severity,source,scope,impact,**details):
    return {"code":code,"severity":severity,"source":source,"scope":scope,"impact":impact,"details":details}

def compatibility_payload_max(model,run_hour,cfg):
    """Maximum lead represented by the compatibility/current models payload.

    This is deliberately not the provider-native or archive acquisition horizon;
    those are owned by full_horizon_contract.
    """
    p=cfg["model_policy"]["compatibility_payload_horizon_by_cycle"][model]
    if "default_max_hours" in p:return int(p["default_max_hours"])
    return int(p["main_max_hours"] if run_hour in p["main_cycle_hours"] else p["other_max_hours"])

def desired_leads(model,cfg):
    return [int(x) for x in cfg["model_policy"]["project_desired_leads"][model]]

def gefs_mature_cycle_archive_exception(payload,run,now):
    """Validate evidence that an over-age GEFS cycle is the newest mature full-horizon cycle.

    This exception affects only whether nominal forecast-age excess is fatal.
    It never makes the selected GEFS run count as current for the independent
    family gate.
    """
    if os.getenv("FULL_VALIDATION","").lower()!="true" or run is None:
        return False,{"reason":"not_full_validation"}
    evidence=(payload.get("provider_selection_evidence") or {}).get("GEFS-control")
    if not isinstance(evidence,dict):
        return False,{"reason":"selection_evidence_missing"}
    if evidence.get("method_version")!="gefs-newest-mature-cycle-selection-v1":
        return False,{"reason":"selection_evidence_version_invalid"}
    if evidence.get("full_horizon_publication_required") is not True:
        return False,{"reason":"far_horizon_publication_not_required"}
    selected=dt(evidence.get("selected_cycle_run_time_utc"))
    if selected!=run:
        return False,{"reason":"selected_cycle_mismatch",
                      "evidence_cycle":selected.isoformat() if selected else None,
                      "record_cycle":run.isoformat()}
    try:
        from mardorf_collector.contracts.full_horizon_contract import maximum_hours
        selected_expected=int(maximum_hours("GEFS-control",run))
        selected_probe=int(evidence.get("selected_publication_probe_lead"))
        selected_declared=int(evidence.get("selected_expected_max_lead_hours"))
    except Exception as exc:
        return False,{"reason":"selected_terminal_horizon_invalid","detail":str(exc)}
    if selected_probe!=selected_expected or selected_declared!=selected_expected:
        return False,{"reason":"selected_terminal_probe_mismatch",
                      "expected_terminal_lead":selected_expected,
                      "declared_terminal_lead":selected_declared,
                      "publication_probe_lead":selected_probe}
    attempts=evidence.get("attempts")
    if not isinstance(attempts,list):
        return False,{"reason":"selection_attempts_missing"}
    by_cycle={}
    for item in attempts:
        if not isinstance(item,dict):
            continue
        cyc=dt(item.get("cycle_run_time_utc"))
        if cyc is not None:
            by_cycle[cyc]=item
    selected_attempt=by_cycle.get(run)
    if not selected_attempt or selected_attempt.get("status")!="published":
        return False,{"reason":"selected_cycle_not_proven_published"}
    if int(selected_attempt.get("publication_probe_lead",-1))!=selected_expected:
        return False,{"reason":"selected_attempt_probe_not_terminal"}

    # GEFS scheduled cycles are 6-hourly. Every newer cycle that is already due
    # must have been explicitly probed at its own native terminal horizon.
    newer_due=[]
    cycle=run+timedelta(hours=6)
    while cycle<=now:
        newer_due.append(cycle)
        cycle+=timedelta(hours=6)
    if not newer_due:
        return False,{"reason":"no_newer_due_cycle_to_explain_age_excess"}
    newer_evidence=[]
    for cyc in newer_due:
        item=by_cycle.get(cyc)
        if not item:
            return False,{"reason":"newer_due_cycle_not_probed","cycle_run_time_utc":cyc.isoformat()}
        try:
            from mardorf_collector.contracts.full_horizon_contract import maximum_hours
            terminal=int(maximum_hours("GEFS-control",cyc))
            probe=int(item.get("publication_probe_lead"))
        except Exception as exc:
            return False,{"reason":"newer_cycle_probe_invalid","cycle_run_time_utc":cyc.isoformat(),"detail":str(exc)}
        if probe!=terminal:
            return False,{"reason":"newer_cycle_not_probed_at_terminal_horizon",
                          "cycle_run_time_utc":cyc.isoformat(),
                          "expected_terminal_lead":terminal,"publication_probe_lead":probe}
        if item.get("status")!="not_published":
            return False,{"reason":"newer_cycle_not_proven_immature",
                          "cycle_run_time_utc":cyc.isoformat(),"status":item.get("status")}
        newer_evidence.append({"cycle_run_time_utc":cyc.isoformat(),
                               "terminal_probe_lead":terminal,
                               "status":"not_published",
                               "http_status":item.get("http_status")})
    return True,{
        "reason":"newest_mature_full_horizon_cycle_proven",
        "selection_method_version":evidence.get("method_version"),
        "selected_cycle_run_time_utc":run.isoformat(),
        "selected_terminal_probe_lead":selected_expected,
        "newer_due_cycles":newer_evidence,
    }


def icon_eu_mature_cycle_archive_exception(payload,run,now):
    """Validate that an over-age ICON-EU run is the newest fully published 120 h main cycle.

    This is a FULL_VALIDATION-only archival exception. It suppresses only the
    nominal-age error; currentness_policy_pass remains false.
    """
    if os.getenv("FULL_VALIDATION","").lower()!="true" or run is None:
        return False,{"reason":"not_full_validation"}
    evidence=(payload.get("provider_selection_evidence") or {}).get("ICON-EU")
    if not isinstance(evidence,dict):
        return False,{"reason":"selection_evidence_missing"}
    if evidence.get("method_version")!="dwd-newest-mature-cycle-selection-v1":
        return False,{"reason":"selection_evidence_version_invalid"}
    if evidence.get("full_horizon_publication_required") is not True:
        return False,{"reason":"full_horizon_publication_not_required"}
    if int(evidence.get("required_lead_hours",-1))!=120:
        return False,{"reason":"required_lead_not_120"}
    selected=dt(evidence.get("selected_cycle_run_time_utc"))
    if run.hour not in (0,6,12,18):
        return False,{"reason":"selected_cycle_is_not_icon_eu_main_cycle","run_hour":run.hour}
    if selected!=run:
        return False,{"reason":"selected_cycle_mismatch",
                      "evidence_cycle":selected.isoformat() if selected else None,
                      "record_cycle":run.isoformat()}
    attempts=evidence.get("attempts")
    if not isinstance(attempts,list):
        return False,{"reason":"selection_attempts_missing"}
    by_cycle={}
    for item in attempts:
        if not isinstance(item,dict):
            continue
        cyc=dt(item.get("cycle_run_time_utc"))
        if cyc is not None:
            by_cycle[cyc]=item
    selected_attempt=by_cycle.get(run)
    if not selected_attempt or selected_attempt.get("status")!="published":
        return False,{"reason":"selected_cycle_not_proven_published"}
    if int(selected_attempt.get("required_lead_hours",-1))!=120:
        return False,{"reason":"selected_cycle_not_probed_at_120h"}

    # ICON-EU full 120 h products are main 6-hourly cycles.
    newer_due=[]
    cycle=run+timedelta(hours=6)
    while cycle<=now:
        newer_due.append(cycle)
        cycle+=timedelta(hours=6)
    if not newer_due:
        return False,{"reason":"no_newer_due_cycle_to_explain_age_excess"}

    newer_evidence=[]
    for cyc in newer_due:
        item=by_cycle.get(cyc)
        if not item:
            return False,{"reason":"newer_due_cycle_not_probed",
                          "cycle_run_time_utc":cyc.isoformat()}
        if int(item.get("required_lead_hours",-1))!=120:
            return False,{"reason":"newer_cycle_not_probed_at_120h",
                          "cycle_run_time_utc":cyc.isoformat()}
        if item.get("status")!="not_published":
            return False,{"reason":"newer_cycle_not_proven_immature",
                          "cycle_run_time_utc":cyc.isoformat(),
                          "status":item.get("status")}
        if item.get("listing_errors"):
            return False,{"reason":"newer_cycle_probe_error",
                          "cycle_run_time_utc":cyc.isoformat(),
                          "listing_errors":item.get("listing_errors")}
        missing=item.get("missing_required_fields")
        if not isinstance(missing,list) or not missing:
            return False,{"reason":"newer_cycle_missingness_evidence_invalid",
                          "cycle_run_time_utc":cyc.isoformat()}
        newer_evidence.append({
            "cycle_run_time_utc":cyc.isoformat(),
            "required_lead_hours":120,
            "status":"not_published",
            "missing_required_fields":missing,
            "max_available_lead_by_field":item.get("max_available_lead_by_field"),
        })
    return True,{
        "reason":"newest_mature_icon_eu_120h_cycle_proven",
        "selection_method_version":evidence.get("method_version"),
        "selected_cycle_run_time_utc":run.isoformat(),
        "selected_required_lead_hours":120,
        "newer_due_cycles":newer_evidence,
    }

def audit_eps_hourly_source(source,run,expected_members=20):
    failures=[];summary=None
    if not isinstance(source,dict):
        return [{"reason":"ensemble_hourly_source_missing"}],summary
    if source.get("model")!="dwd_icon_d2_eps":
        failures.append({"reason":"hourly_source_model_mismatch","value":source.get("model")})
    if source.get("cycle_evidence")!="stable_provider_metadata_association":
        failures.append({"reason":"hourly_source_cycle_evidence_invalid","value":source.get("cycle_evidence")})
    sr=dt(source.get("run_time_utc"))
    if sr is None or run is None or sr!=run:
        failures.append({"reason":"hourly_source_run_time_mismatch",
                         "hourly_run_time_utc":sr.isoformat() if sr else None,
                         "eps_run_time_utc":run.isoformat() if run else None})
    raw_times=source.get("times_utc")
    times=[dt(x) for x in raw_times] if isinstance(raw_times,list) else []
    if not times or any(x is None for x in times):
        failures.append({"reason":"hourly_source_time_axis_invalid"})
        times=[]
    elif any(x.minute or x.second or x.microsecond for x in times):
        failures.append({"reason":"hourly_source_non_hour_boundary"})
    elif times!=sorted(times) or len(times)!=len(set(times)):
        failures.append({"reason":"hourly_source_time_axis_unordered_or_duplicate"})
    columns=source.get("columns") if isinstance(source.get("columns"),dict) else {}
    expected_ids=list(range(expected_members))
    def members(field):
        raw=columns.get(field)
        if not isinstance(raw,dict):return {}
        out={}
        for key,values in raw.items():
            try:member=int(key)
            except (TypeError,ValueError):continue
            out[member]=values
        return out
    core={field:members(field) for field in ("wind_speed_10m","wind_direction_10m","wind_gusts_10m")}
    for field,vals in core.items():
        if sorted(vals)!=expected_ids:
            failures.append({"reason":"hourly_source_member_identity_mismatch",
                             "field":field,"expected_member_ids":expected_ids,"received_member_ids":sorted(vals)})
        for member,values in vals.items():
            if not isinstance(values,list) or (times and len(values)!=len(times)):
                failures.append({"reason":"hourly_source_column_length_mismatch",
                                 "field":field,"member":member,
                                 "values":len(values) if isinstance(values,list) else None,
                                 "times":len(times)})
    registry_mode=source.get("registry_version")=="relevant-meteorology-v1"
    weather_completeness={}
    if registry_mode:
        expected_semantics={
            "wind_speed_10m":"wind_speed_10m",
            "wind_direction_10m":"wind_direction_10m",
            "wind_gusts_10m":"wind_gust_10m",
            "precipitation":"total_precipitation",
            "cape":"cape",
            "temperature_2m":"air_temperature_2m",
            "relative_humidity_2m":"relative_humidity_2m",
            "dew_point_2m":"dewpoint_temperature_2m",
            "pressure_msl":"mean_sea_level_pressure",
            "surface_pressure":"surface_pressure",
            "cloud_cover":"total_cloud_cover",
            "shortwave_radiation":"surface_downward_shortwave",
        }
        specs=source.get("field_specs") if isinstance(source.get("field_specs"),dict) else {}
        declared=source.get("field_completeness_0_48") if isinstance(source.get("field_completeness_0_48"),dict) else {}
        for field,semantic in expected_semantics.items():
            vals=members(field)
            if sorted(vals)!=expected_ids:
                failures.append({"reason":"hourly_source_weather_member_identity_mismatch",
                                 "field":field,"expected_member_ids":expected_ids,
                                 "received_member_ids":sorted(vals)})
            spec=specs.get(field) if isinstance(specs.get(field),dict) else {}
            if spec.get("semantic_id")!=semantic:
                failures.append({"reason":"hourly_source_registry_semantic_mismatch",
                                 "field":field,"expected_semantic_id":semantic,
                                 "value":spec.get("semantic_id")})
            if spec.get("field_provider_product")!="open_meteo:dwd_icon_d2_eps":
                failures.append({"reason":"hourly_source_field_product_mismatch",
                                 "field":field,"value":spec.get("field_provider_product")})
            for member,values in vals.items():
                if not isinstance(values,list) or (times and len(values)!=len(times)):
                    failures.append({"reason":"hourly_source_weather_column_length_mismatch",
                                     "field":field,"member":member,
                                     "values":len(values) if isinstance(values,list) else None,
                                     "times":len(times)})
        unsupported=source.get("unsupported_registry_semantics")
        cin=[x for x in unsupported if isinstance(x,dict) and x.get("semantic_id")=="cin"] if isinstance(unsupported,list) else []
        if len(cin)!=1 or cin[0].get("availability_status")!="unsupported_by_provider_or_product":
            failures.append({"reason":"hourly_source_cin_unsupported_declaration_missing",
                             "value":unsupported})
    missing_hours=[];invalid_values=[]
    if run and times:
        index={x:i for i,x in enumerate(times)}
        for hour in range(49):
            target=run+timedelta(hours=hour);i=index.get(target)
            if i is None:
                missing_hours.append(hour);continue
            for field,vals in core.items():
                for member in expected_ids:
                    values=vals.get(member)
                    value=values[i] if isinstance(values,list) and i<len(values) else None
                    valid=finite(value)
                    if field in ("wind_speed_10m","wind_gusts_10m"):valid=valid and float(value)>=0
                    if field=="wind_direction_10m":valid=valid and 0<=float(value)<=360
                    if not valid:
                        invalid_values.append({"lead_hour":hour,"field":field,"member":member,"value":value})
                        if len(invalid_values)>=20:break
                if len(invalid_values)>=20:break
            if len(invalid_values)>=20:break
    if missing_hours:
        failures.append({"reason":"hourly_source_required_hours_missing","missing_lead_hours":missing_hours})
    if invalid_values:
        failures.append({"reason":"hourly_source_wind_core_values_invalid","examples":invalid_values,
                         "example_limit":20})
    requested=source.get("requested_coordinate") if isinstance(source.get("requested_coordinate"),dict) else {}
    returned=source.get("returned_coordinate") if isinstance(source.get("returned_coordinate"),dict) else {}
    rlat=returned.get("latitude");rlon=returned.get("longitude")
    if requested.get("latitude")!=52.4942 or requested.get("longitude")!=9.3418:
        failures.append({"reason":"hourly_source_requested_coordinate_mismatch","value":requested})
    if not finite(rlat) or not finite(rlon) or abs(float(rlat)-52.4942)>.05 or abs(float(rlon)-9.3418)>.08:
        failures.append({"reason":"hourly_source_returned_coordinate_implausible","value":returned})
    spatial=source.get("spatial_provenance_evidence") if isinstance(source.get("spatial_provenance_evidence"),dict) else {}
    if source.get("spatial_provenance_verified") is not True or spatial.get("verified") is not True:
        failures.append({"reason":"hourly_source_spatial_provenance_unverified",
                         "evidence":spatial})
    if source.get("dwd_eps_native_grid_identity_verified") is not True:
        failures.append({"reason":"hourly_source_dwd_eps_native_grid_identity_unverified",
                         "evidence":spatial.get("eps_native_grid_identity")})
    native=spatial.get("native_coordinate_proof") or {}
    native_identity=spatial.get("eps_native_grid_identity") or {}
    direct_native=(source.get("native_grid_parity_verified") is True
        and spatial.get("native_coordinate_parity_claimed") is True
        and native.get("verified") is True
        and native.get("method")=="provider_returned_point_matched_to_native_eps_cell_v1"
        and native.get("grid_uuid")==native_identity.get("uuid_of_horizontal_grid")
        and type(native.get("native_cell_index")) is int
        and 0<=native["native_cell_index"]<int(native_identity.get("number_of_data_points",0))
        and finite(native.get("coordinate_difference_m"))
        and 0<=native["coordinate_difference_m"]<=1 and native.get("tolerance_m")==1)
    if source.get("dwd_regular_grid_coordinate_parity_verified") is not True and not direct_native:
        failures.append({"reason":"hourly_source_dwd_regular_grid_coordinate_parity_unverified",
                         "evidence":spatial})
    binding=source.get("response_run_binding") if isinstance(source.get("response_run_binding"),dict) else {}
    if source.get("response_bound_run_identity_verified") is not True and binding.get("status")!="strong_indirect_bracketed_not_provider_embedded":
        failures.append({"reason":"hourly_source_run_binding_evidence_missing",
                         "response_bound_run_identity_verified":source.get("response_bound_run_identity_verified"),
                         "response_run_binding":binding})
    summary={
        "present":True,"run_time_utc":sr.isoformat() if sr else None,
        "time_count":len(times),"required_run_through_48h_complete":not missing_hours and bool(times),
        "expected_member_count":expected_members,
        "response_sha256":source.get("response_sha256"),
        "retrieved_at_utc":source.get("retrieved_at_utc"),
        "requested_coordinate":requested,"returned_coordinate":returned,
        "cycle_evidence":source.get("cycle_evidence"),
        "spatial_provenance_verified":source.get("spatial_provenance_verified"),
        "dwd_eps_native_grid_identity_verified":source.get("dwd_eps_native_grid_identity_verified"),
        "dwd_regular_grid_coordinate_parity_verified":source.get("dwd_regular_grid_coordinate_parity_verified"),
        "native_grid_parity_verified":source.get("native_grid_parity_verified"),
        "response_bound_run_identity_verified":source.get("response_bound_run_identity_verified"),
        "response_run_binding_status":binding.get("status"),
        "registry_version":source.get("registry_version"),
        "member_weather_field_count":len(source.get("field_specs") or {}) if registry_mode else len(columns),
        "field_completeness_0_48":source.get("field_completeness_0_48") if registry_mode else None,
        "unsupported_registry_semantics":source.get("unsupported_registry_semantics") if registry_mode else None,
        "failure_count":len(failures),
    }
    return failures,summary

GEFS_FULL_METHOD_VERSION="phase2f3-gefs-full-members-v1"
GEFS_FULL_POLICY_VERSION="gefs-sparse-00z-policy-v1"
GEFS_FULL_MEMBERS=("c00",)+tuple(f"p{x:02d}" for x in range(1,31))
GEFS_FULL_NEAR=(60,72,84,96,108,120,144,168,192,216,240)
GEFS_FULL_FAR=(288,336,384,432,480,528,576,624,672,720,768,816,840)
GEFS_FULL_LEADS=GEFS_FULL_NEAR+GEFS_FULL_FAR


def audit_gefs_full_member_source(payload,expected_spot):
    """Validate optional archive-only sparse GEFS full-member evidence."""
    failures=[]
    source=payload.get("gefs_full_member_source")
    attempt=payload.get("gefs_full_member_attempt")
    if source is None:
        return failures,None,attempt
    if not isinstance(source,dict):
        return [{"reason":"source_not_object"}],{"present":True},attempt
    if source.get("method_version")!=GEFS_FULL_METHOD_VERSION:
        failures.append({"reason":"method_version_mismatch","value":source.get("method_version")})
    if source.get("policy_version")!=GEFS_FULL_POLICY_VERSION:
        failures.append({"reason":"policy_version_mismatch","value":source.get("policy_version")})
    if source.get("ensemble_system_id")!="NOAA_GEFS":
        failures.append({"reason":"ensemble_system_id_mismatch","value":source.get("ensemble_system_id")})
    run=dt(source.get("run_time_utc"))
    if run is None or run.hour!=0:
        failures.append({"reason":"run_not_00z","run_time_utc":source.get("run_time_utc")})
    expected_ids=list(GEFS_FULL_MEMBERS)
    if source.get("expected_member_ids")!=expected_ids:
        failures.append({"reason":"expected_member_ids_mismatch","value":source.get("expected_member_ids")})
    roles=source.get("member_roles") if isinstance(source.get("member_roles"),dict) else {}
    expected_roles={"c00":"control_member",**{f"p{x:02d}":"perturbed_member" for x in range(1,31)}}
    if roles!=expected_roles:
        failures.append({"reason":"member_roles_mismatch"})
    leads=source.get("leads_hours")
    if leads!=list(GEFS_FULL_LEADS):
        failures.append({"reason":"sparse_lead_policy_mismatch","value":leads})
    if source.get("native_time_policy")!="provider-native selected sparse times only; no interpolation":
        failures.append({"reason":"native_time_policy_mismatch","value":source.get("native_time_policy")})
    expected_requests=len(GEFS_FULL_MEMBERS)*len(GEFS_FULL_LEADS)
    if source.get("collection_status")!="complete":
        failures.append({"reason":"collection_not_complete","value":source.get("collection_status")})
    if source.get("expected_request_count")!=expected_requests or source.get("received_request_count")!=expected_requests:
        failures.append({"reason":"request_count_mismatch","expected":expected_requests,
                         "recorded_expected":source.get("expected_request_count"),
                         "recorded_received":source.get("received_request_count")})
    records=source.get("records")
    if not isinstance(records,list) or len(records)!=expected_requests:
        failures.append({"reason":"record_inventory_mismatch","expected":expected_requests,
                         "received":len(records) if isinstance(records,list) else None})
        records=[] if not isinstance(records,list) else records
    seen=set();points={};semantic_counts=Counter();field_count=0
    allowed={
        "gefs_0p25s":{"wind_u_10m","wind_v_10m","wind_gust_10m","total_precipitation","total_cloud_cover","cape","cin"},
        "gefs_0p50a":{"wind_u_10m","wind_v_10m","total_precipitation","total_cloud_cover","cape","cin"},
    }
    required_semantics={k:set(v) for k,v in allowed.items()}
    per_unit_semantics={}
    for record in records:
        if not isinstance(record,dict):
            failures.append({"reason":"record_not_object"});continue
        member=str(record.get("member_id"))
        try:lead=int(record.get("lead_hours"))
        except Exception:
            failures.append({"reason":"invalid_lead","value":record.get("lead_hours")});continue
        product=str(record.get("provider_product"))
        key=(member,lead)
        if key in seen:
            failures.append({"reason":"duplicate_member_lead","member_id":member,"lead_hours":lead})
        seen.add(key)
        wanted="gefs_0p25s" if lead in GEFS_FULL_NEAR else "gefs_0p50a" if lead in GEFS_FULL_FAR else None
        if member not in GEFS_FULL_MEMBERS or wanted is None or product!=wanted:
            failures.append({"reason":"member_lead_product_mismatch","member_id":member,"lead_hours":lead,
                             "provider_product":product,"expected_product":wanted})
        valid=dt(record.get("valid_time_utc"))
        if run is not None and (valid is None or abs((valid-run).total_seconds()/3600-lead)>0.01):
            failures.append({"reason":"valid_time_mismatch","member_id":member,"lead_hours":lead,
                             "valid_time_utc":record.get("valid_time_utc")})
        if record.get("request_status")!="received":
            failures.append({"reason":"request_not_received","member_id":member,"lead_hours":lead,
                             "status":record.get("request_status")})
        point=record.get("returned_coordinate") if isinstance(record.get("returned_coordinate"),dict) else {}
        lat=point.get("latitude");lon=point.get("longitude")
        if not finite(lat) or not finite(lon):
            failures.append({"reason":"grid_point_missing","member_id":member,"lead_hours":lead})
        else:
            pad=.30 if product=="gefs_0p25s" else .75
            if abs(float(lat)-float(expected_spot["latitude"]))>pad+0.01 or abs(float(lon)-float(expected_spot["longitude"]))>pad+0.01:
                failures.append({"reason":"grid_point_implausible","member_id":member,"lead_hours":lead,
                                 "provider_product":product,"point":point})
            prior=points.get(product)
            rounded=(round(float(lat),6),round(float(lon),6))
            if prior is None:points[product]=rounded
            elif prior!=rounded:
                failures.append({"reason":"grid_point_changes_within_product","provider_product":product,
                                 "expected":prior,"observed":rounded})
        fields=record.get("fields")
        if not isinstance(fields,list) or not fields:
            failures.append({"reason":"fields_missing","member_id":member,"lead_hours":lead});continue
        unit_semantics=set()
        for field in fields:
            if not isinstance(field,dict):
                failures.append({"reason":"field_not_object","member_id":member,"lead_hours":lead});continue
            semantic=str(field.get("semantic_id") or "")
            unit_semantics.add(semantic);semantic_counts[semantic]+=1;field_count+=1
            if semantic not in allowed.get(product,set()):
                failures.append({"reason":"unapproved_semantic","member_id":member,"lead_hours":lead,
                                 "provider_product":product,"semantic_id":semantic})
            if not field.get("field_key") or not field.get("parameter_native") or not finite(field.get("value_native")):
                failures.append({"reason":"native_field_identity_or_value_incomplete","member_id":member,
                                 "lead_hours":lead,"semantic_id":semantic})
            if str(field.get("field_provider_product"))!=product:
                failures.append({"reason":"field_product_mismatch","member_id":member,"lead_hours":lead,
                                 "semantic_id":semantic})
        per_unit_semantics[key]=unit_semantics
        if unit_semantics!=required_semantics.get(product,set()):
            failures.append({"reason":"unit_semantic_set_mismatch","member_id":member,"lead_hours":lead,
                             "provider_product":product,
                             "expected":sorted(required_semantics.get(product,set())),
                             "observed":sorted(unit_semantics)})
    if len(seen)!=expected_requests:
        failures.append({"reason":"unique_member_lead_count_mismatch","expected":expected_requests,"received":len(seen)})
    omissions=source.get("policy_omissions")
    omission_ok=False
    if isinstance(omissions,list):
        for item in omissions:
            if not isinstance(item,dict):continue
            if (
                item.get("provider_product")=="gefs_0p50b"
                and item.get("availability_status")=="not_requested_by_policy"
                and item.get("member_ids")==[f"p{x:02d}" for x in range(1,31)]
                and item.get("leads_hours")==list(GEFS_FULL_FAR)
            ):
                omission_ok=True
    if not omission_ok:
        failures.append({"reason":"pgrb2b_policy_omission_missing_or_wrong"})
    metrics=source.get("request_metrics") if isinstance(source.get("request_metrics"),dict) else {}
    traffic=source.get("traffic_metrics") if isinstance(source.get("traffic_metrics"),dict) else {}
    response_bytes=traffic.get("total_response_bytes",metrics.get("response_bytes"))
    total_requests=traffic.get("total_requests",metrics.get("requests"))
    qa=source.get("provider_summary_qa")
    qa_status=None
    if not isinstance(qa,dict):
        failures.append({"reason":"provider_summary_qa_missing"})
    else:
        qa_status=qa.get("status")
        if qa.get("method_version")!="gefs-provider-summary-qa-v1":
            failures.append({"reason":"provider_summary_qa_method_mismatch","value":qa.get("method_version")})
        if qa.get("member_semantics")!="NOAA geavg/gespr empirically reproduce p01-p30 only; c00 excluded":
            failures.append({"reason":"provider_summary_member_semantics_mismatch","value":qa.get("member_semantics")})
        if qa.get("spread_semantics")!="sample standard deviation over p01-p30 (N-1 denominator)":
            failures.append({"reason":"provider_summary_spread_semantics_mismatch","value":qa.get("spread_semantics")})
        if qa_status=="pass":
            comparisons=qa.get("comparisons") if isinstance(qa.get("comparisons"),list) else []
            if len(comparisons)!=4:
                failures.append({"reason":"provider_summary_comparison_count_mismatch","count":len(comparisons)})
            for item in comparisons:
                if not isinstance(item,dict):
                    failures.append({"reason":"provider_summary_comparison_not_object"});continue
                if item.get("lead_hours") not in (120,240) or item.get("semantic_id") not in ("wind_u_10m","wind_v_10m"):
                    failures.append({"reason":"provider_summary_comparison_identity_mismatch","comparison":item})
                    continue
                if item.get("member_set")!="p01-p30" or item.get("member_count")!=30:
                    failures.append({"reason":"provider_summary_member_set_mismatch","comparison":item})
                md=item.get("mean_abs_delta");sd=item.get("spread_abs_delta")
                mt=item.get("mean_tolerance_ms");st=item.get("spread_tolerance_ms")
                if not all(finite(x) for x in (md,sd,mt,st)):
                    failures.append({"reason":"provider_summary_tolerance_evidence_nonfinite","comparison":item})
                elif float(md)>float(mt)+1e-12 or float(sd)>float(st)+1e-12 or item.get("status")!="pass":
                    failures.append({"reason":"provider_summary_tolerance_failed","comparison":item})
        elif qa_status=="fail":
            failures.append({"reason":"provider_summary_qa_failed","qa":qa})
        elif qa_status!="unavailable":
            failures.append({"reason":"provider_summary_qa_status_invalid","value":qa_status})
    summary={
        "present":True,
        "method_version":source.get("method_version"),"policy_version":source.get("policy_version"),
        "run_time_utc":run.isoformat() if run else source.get("run_time_utc"),
        "member_count":len(expected_ids),"lead_count":len(GEFS_FULL_LEADS),
        "expected_request_count":expected_requests,"record_count":len(records),
        "unique_member_lead_count":len(seen),"field_count":field_count,
        "semantic_counts":dict(sorted(semantic_counts.items())),
        "grid_points_by_product":{k:{"latitude":v[0],"longitude":v[1]} for k,v in sorted(points.items())},
        "request_metrics":metrics,
        "traffic_metrics":traffic,
        "total_requests":total_requests,
        "request_budget":800,
        "request_budget_pass":finite(total_requests) and float(total_requests)<=800,
        "network_budget_bytes":2*1024*1024,
        "network_budget_pass":finite(response_bytes) and float(response_bytes)<=2*1024*1024,
        "provider_summary_qa":qa,
        "provider_summary_qa_status":qa_status,
        "failure_count":len(failures),
        "policy_omission_verified":omission_ok,
    }
    return failures,summary,attempt


def full_horizon_coverage_issue(archive_summary, full_validation):
    """Classify archive coverage without hiding required-horizon gaps."""
    if archive_summary.get("horizon_status") != "complete" and full_validation:
        return ("FULL_HORIZON_REQUIRED_COVERAGE_INCOMPLETE", "ERROR",
                "Full-model validation requires complete provider-native horizon coverage for every required model cycle.")
    if archive_summary.get("status") != "complete":
        return ("FULL_HORIZON_ARCHIVE_PARTIAL", "WARN",
                "Full-horizon collection has explicit gaps or optional-field gaps; legacy analysis coverage is unchanged.")
    return None

def make_report(kind,now,sources,issues,usable,extra):
    counts=Counter(x["severity"] for x in issues)
    status="PASS" if counts["ERROR"]==0 and counts["WARN"]==0 else "PASS_WITH_WARNINGS" if counts["ERROR"]==0 else "FAIL"
    return {
        "schema_version":1,"method_version":str(POLICY_VERSION),"kind":kind,
        "generated_at_utc":now.isoformat(),"status":status,
        "error_count":counts["ERROR"],"warning_count":counts["WARN"],"info_count":counts["INFO"],
        "bundle_ready_for_private_revalidation":bool(usable and counts["ERROR"]==0),
        "sources":sources,"issues":issues,**extra
    }

def audit_models(path,cfg,now):
    issues=[];sources={}
    if not path.exists():
        issues.append(issue("MODEL_BUNDLE_FILE_NOT_CREATED","ERROR","collector","bundle",
            "No model payload exists to transfer or ingest.",path=str(path)))
        return make_report("models",now,sources,issues,False,{"input_file_present":False})
    try:raw=path.read_bytes()
    except Exception as e:
        issues.append(issue("MODEL_BUNDLE_FILE_READ_FAILED","ERROR","collector","bundle",
            "The model payload exists but cannot be read.",exception_type=type(e).__name__,exception_message=str(e),path=str(path)))
        return make_report("models",now,sources,issues,False,{"input_file_present":True})
    input_meta={"input_payload_sha256":hashlib.sha256(raw).hexdigest(),"input_payload_bytes":len(raw)}
    try:d=json.loads(raw.decode("utf-8"))
    except Exception as e:
        issues.append(issue("MODEL_BUNDLE_JSON_INVALID","ERROR","collector","bundle",
            "The model payload cannot be parsed.",exception_type=type(e).__name__,exception_message=str(e),path=str(path)))
        return make_report("models",now,sources,issues,False,{"input_file_present":True,**input_meta})

    spot=d.get("spot") if isinstance(d.get("spot"),dict) else {}
    expected_spot=cfg["model_policy"]["spot"]
    spot_lat=spot.get("lat",spot.get("latitude"));spot_lon=spot.get("lon",spot.get("longitude"))
    if (not finite(spot_lat) or not finite(spot_lon)
            or abs(float(spot_lat)-float(expected_spot["latitude"]))>1e-6
            or abs(float(spot_lon)-float(expected_spot["longitude"]))>1e-6):
        issues.append(issue("MODEL_SPOT_IDENTITY_MISMATCH","ERROR","collector","spot_identity",
            "The model payload coordinates do not match the configured Mardorf collection point.",
            observed_spot=spot,expected_spot=expected_spot))

    mode=d.get("mode","unknown")
    retrieval=dt(d.get("retrieved_at_utc") or d.get("horizon_extension_retrieved_at_utc") or d.get("dwd_additional_retrieved_at_utc") or d.get("extended_retrieved_at_utc"))
    if retrieval is None:
        issues.append(issue("RETRIEVAL_TIMESTAMP_INVALID","ERROR","collector","bundle",
            "Bundle age and chronology cannot be verified.",value=d.get("retrieved_at_utc")))
    else:
        future_min=(retrieval-now).total_seconds()/60
        tol=float(cfg["model_policy"]["retrieval_timestamp_future_tolerance_minutes"])
        if future_min>tol:
            issues.append(issue("RETRIEVAL_TIMESTAMP_IN_FUTURE","ERROR","collector","bundle",
                "Bundle chronology is implausible.",retrieved_at_utc=retrieval.isoformat(),
                checked_at_utc=now.isoformat(),future_offset_minutes=round(future_min,2),allowed_future_minutes=tol))

    qerrors=list((d.get("quality") or {}).get("errors") or [])
    all_attempts=[x for x in (d.get("provider_attempts") or []) if isinstance(x,dict)]
    for model in cfg["model_policy"]["project_desired_leads"]:
        recs=[r for r in (d.get("models") or {}).get(model,[]) if isinstance(r,dict)]
        lead_rows={};duplicates=[];invalid_lead_rows=[];model_identity_failures=[]
        for r in recs:
            if r.get("model")!=model:
                model_identity_failures.append({"declared_model":r.get("model"),"expected_model":model,
                                                "forecast_lead_hours":r.get("forecast_lead_hours")})
            try:lead=int(r.get("forecast_lead_hours"))
            except Exception:
                invalid_lead_rows.append({"valid_time_utc":r.get("valid_time_utc"),"value":r.get("forecast_lead_hours")});continue
            if lead in lead_rows:duplicates.append(lead)
            else:lead_rows[lead]=r

        run_values=sorted({str(r.get("run_time_utc")) for r in recs if r.get("run_time_utc")})
        run=dt(run_values[0]) if len(run_values)==1 else None
        run_hour=run.hour if run else None
        pmax=compatibility_payload_max(model,run_hour,cfg) if run_hour is not None else None
        if mode=="test":
            expected=[0,12,24,30,36,42,48] if model in ("ICON-D2","GFS") else [0,12,24,36,48]
            project_gap=[]
        else:
            expected=[x for x in desired_leads(model,cfg) if pmax is not None and x<=pmax]
            project_gap=[x for x in desired_leads(model,cfg) if pmax is not None and x>pmax]
        got=sorted(lead_rows);missing=sorted(set(expected)-set(got));extra=sorted(set(got)-set(expected))
        field_failures=[];timestamp_failures=[];coordinate_failures=[];coordinate_points=set();critical_source_errors=[];optional_source_warnings=[];outside_horizon_records=[]
        expected_set=set(expected)
        optional_fields=set(cfg["model_policy"].get("optional_weather_context_fields",[]))
        for lead,r in sorted(lead_rows.items()):
            in_provider_scope=lead in expected_set
            if not in_provider_scope:
                outside_horizon_records.append({"lead_hours":lead,"reason":"outside_selected_provider_cycle_horizon"})
            pt=r.get("forecast_coordinate_or_grid_point") if isinstance(r.get("forecast_coordinate_or_grid_point"),dict) else {}
            plat=pt.get("latitude",pt.get("lat"));plon=pt.get("longitude",pt.get("lon"))
            if in_provider_scope:
                if (not finite(plat) or not finite(plon)
                        or abs(float(plat)-float(expected_spot["latitude"]))>0.30
                        or abs(float(plon)-float(expected_spot["longitude"]))>0.30):
                    coordinate_failures.append({
                        "lead_hours":lead,"reason":"missing_or_implausible_forecast_grid_point",
                        "forecast_coordinate_or_grid_point":pt})
                else:
                    coordinate_points.add((round(float(plat),6),round(float(plon),6)))
            der=r.get("derived") if isinstance(r.get("derived"),dict) else {}
            req=list(cfg["model_policy"]["required_derived_fields"])
            if model in cfg["model_policy"]["required_gust_models"]:req.append("gust_ms")
            absent=[f for f in req if not finite(der.get(f))]
            if in_provider_scope and absent:field_failures.append({"lead_hours":lead,"fields":absent})
            rt=dt(r.get("run_time_utc"));vt=dt(r.get("valid_time_utc"))
            if rt is None or vt is None:
                timestamp_failures.append({"lead_hours":lead,"run_time_utc":r.get("run_time_utc"),"valid_time_utc":r.get("valid_time_utc"),"reason":"unparseable_timestamp"})
            else:
                actual=(vt-rt).total_seconds()/3600
                if abs(actual-lead)>0.06:
                    timestamp_failures.append({"lead_hours":lead,"run_time_utc":rt.isoformat(),"valid_time_utc":vt.isoformat(),"actual_lead_hours":round(actual,3),"reason":"declared_lead_differs_from_run_to_valid_interval"})
            def record_problem(item,field=None):
                if not in_provider_scope:
                    item["classification"]="outside_provider_cycle_horizon"
                    outside_horizon_records.append(item)
                elif field in optional_fields:
                    item["classification"]="optional_weather_context"
                    optional_source_warnings.append(item)
                else:
                    item["classification"]="wind_or_record_critical"
                    critical_source_errors.append(item)
            if r.get("error"):
                record_problem({"lead_hours":lead,"location":"record","message":str(r["error"])})
            if r.get("error_type") or r.get("error_message"):
                record_problem({"lead_hours":lead,"location":"record","exception_type":r.get("error_type"),"message":r.get("error_message")})
            if r.get("derive_error"):
                record_problem({"lead_hours":lead,"location":"derive","message":str(r["derive_error"])})
            if r.get("derive_error_type") or r.get("derive_error_message"):
                record_problem({"lead_hours":lead,"location":"derive","exception_type":r.get("derive_error_type"),"message":r.get("derive_error_message")})
            vals=r.get("values")
            if isinstance(vals,dict):
                for key,val in vals.items():
                    if isinstance(val,dict) and val.get("error"):
                        record_problem({"lead_hours":lead,"location":"values."+key,"message":str(val["error"])},key)
                    if isinstance(val,dict) and (val.get("error_type") or val.get("error_message")):
                        record_problem({"lead_hours":lead,"location":"values."+key,"exception_type":val.get("error_type"),"message":val.get("error_message"),"source_url":val.get("source_url")},key)

        if len(coordinate_points)>1:
            coordinate_failures.append({
                "reason":"forecast_grid_point_changes_within_model_run",
                "observed_points":[{"latitude":x[0],"longitude":x[1]} for x in sorted(coordinate_points)]})
        coordinate_summary=(
            {"latitude":next(iter(coordinate_points))[0],"longitude":next(iter(coordinate_points))[1]}
            if len(coordinate_points)==1 else None)
        identity_failures=[];member_failures=[];identity_summary=None;member_count_by_lead={}
        hourly_source_failures=[];hourly_source_summary=None
        native_eps = model=="ICON-D2-EPS" and bool(recs) and all(
            r.get('native_source_version')=='dwd-native-eps-source-v1' for r in recs)
        if native_eps:
            from mardorf_collector.providers.native_eps import audit_source
            identity_failures,identity_summary=audit_source(d.get('native_eps_source'),run,expected)
            for lead,record in lead_rows.items():
                members=record.get('members',[])
                if sorted(m.get('member') for m in members)!=list(range(1,21)):
                    member_failures.append({'lead_hours':lead,'reason':'original_native_member_ids_required'})
            hourly_source_summary={'status':'native_3h_source_separate_from_legacy_API_hourly',
                'legacy_v15_compatible':False,'frozen_candidate_applied':False}
        if model=="ICON-D2-EPS" and recs and not native_eps:
            ecfg=(cfg["model_policy"].get("ensemble_identity") or {}).get("ICON-D2-EPS") or {}
            expected_members=int(ecfg.get("expected_member_count",20))
            required_status=str(ecfg.get("required_verification_status","verified_stable_metadata_and_dwd_cycle"))
            min_settle=float(ecfg.get("minimum_open_meteo_settling_seconds",600))
            identities=[r.get("source_run_identity") for r in recs if isinstance(r.get("source_run_identity"),dict)]
            if len(identities)!=len(recs):
                identity_failures.append({
                    "reason":"source_run_identity_missing_on_some_records",
                    "record_count":len(recs),"identity_record_count":len(identities)})
            if not identities:
                identity_failures.append({"reason":"source_run_identity_missing","record_count":len(recs)})
            else:
                first=identities[0]
                identity_summary={
                    "verification_status":first.get("verification_status"),
                    "run_time_utc":first.get("run_time_utc"),
                    "metadata_before":first.get("metadata_before"),
                    "metadata_after":first.get("metadata_after"),
                    "settling_age_seconds_at_request":first.get("settling_age_seconds_at_request"),
                    "minimum_settling_seconds":first.get("minimum_settling_seconds"),
                    "dwd_cycle_confirmation_url":first.get("dwd_cycle_confirmation_url"),
                    "expected_member_ids":first.get("expected_member_ids"),
                    "source_timestamp_semantics":first.get("source_timestamp_semantics"),
                }
                id_runs=sorted({str(x.get("run_time_utc")) for x in identities if x.get("run_time_utc")})
                statuses=sorted({str(x.get("verification_status")) for x in identities if x.get("verification_status")})
                if statuses!=[required_status]:
                    identity_failures.append({"reason":"verification_status_mismatch","required":required_status,"observed":statuses})
                if len(id_runs)!=1 or (run and dt(id_runs[0])!=run):
                    identity_failures.append({"reason":"identity_run_time_mismatch","bundle_run_time_utc":run.isoformat() if run else None,"identity_run_times":id_runs})
                mb=first.get("metadata_before") if isinstance(first.get("metadata_before"),dict) else {}
                ma=first.get("metadata_after") if isinstance(first.get("metadata_after"),dict) else {}
                before=dt(mb.get("last_run_initialisation_time_utc"));after=dt(ma.get("last_run_initialisation_time_utc"))
                if before is None or after is None or before!=after or (run and before!=run):
                    identity_failures.append({"reason":"metadata_before_after_run_mismatch",
                                              "metadata_before_run_utc":before.isoformat() if before else None,
                                              "metadata_after_run_utc":after.isoformat() if after else None,
                                              "bundle_run_utc":run.isoformat() if run else None})
                settle=first.get("settling_age_seconds_at_request")
                if not finite(settle) or float(settle)<min_settle:
                    identity_failures.append({"reason":"metadata_settling_period_not_met","observed_seconds":settle,"required_seconds":min_settle})
                expected_ids=first.get("expected_member_ids")
                if expected_ids!=list(range(expected_members)):
                    identity_failures.append({"reason":"expected_member_identity_set_mismatch",
                                              "expected_ids":list(range(expected_members)),"recorded_ids":expected_ids})
                if not first.get("dwd_cycle_confirmation_url"):
                    identity_failures.append({"reason":"dwd_cycle_confirmation_missing"})
                critical_identity_keys=(
                    "verification_status","run_time_utc","metadata_before","metadata_after",
                    "settling_age_seconds_at_request","minimum_settling_seconds",
                    "dwd_cycle_confirmation_url","expected_member_ids",
                )
                for rec in recs:
                    ident=rec.get("source_run_identity")
                    if not isinstance(ident,dict):
                        continue
                    mismatched=[k for k in critical_identity_keys if ident.get(k)!=first.get(k)]
                    if mismatched:
                        identity_failures.append({
                            "reason":"source_run_identity_not_identical_across_leads",
                            "lead_hours":rec.get("forecast_lead_hours"),
                            "mismatched_fields":mismatched,
                        })
            for lead,r in sorted(lead_rows.items()):
                if lead not in expected_set:continue
                members=r.get("members") if isinstance(r.get("members"),list) else []
                member_ids=sorted(m.get("member") for m in members if isinstance(m,dict) and isinstance(m.get("member"),int))
                member_count_by_lead[str(lead)]=len(member_ids)
                der=r.get("derived") if isinstance(r.get("derived"),dict) else {}
                stat=r.get("ensemble_statistics") if isinstance(r.get("ensemble_statistics"),dict) else {}
                incomplete_core_members=[]
                for member in members:
                    if not isinstance(member,dict):
                        continue
                    missing_core=[name for name in ("wind_speed_ms","wind_direction_deg","gust_ms")
                                  if not finite(member.get(name))]
                    if missing_core:
                        incomplete_core_members.append({
                            "member":member.get("member"),"missing_or_invalid_fields":missing_core})
                if (member_ids!=list(range(expected_members))
                        or der.get("ensemble_member_count")!=expected_members
                        or stat.get("member_count")!=expected_members
                        or incomplete_core_members):
                    member_failures.append({"lead_hours":lead,"expected_member_ids":list(range(expected_members)),
                                            "received_member_ids":member_ids,
                                            "derived_member_count":der.get("ensemble_member_count"),
                                            "statistics_member_count":stat.get("member_count"),
                                            "incomplete_wind_core_members":incomplete_core_members})
            if mode=="production":
                hourly_source_failures,hourly_source_summary=audit_eps_hourly_source(
                    d.get("ensemble_hourly_source"),run,expected_members)
        model_qerrors=[str(x) for x in qerrors if str(x).startswith(model+":")]
        if len(run_values)==0:
            issues.append(issue("MODEL_RUN_TIME_NOT_PRESENT","ERROR",model,"run_identity",
                "No model cycle can be identified for this source.",record_count=len(recs)))
        elif len(run_values)>1:
            issues.append(issue("MULTIPLE_MODEL_RUNS_IN_ONE_SOURCE_BUNDLE","ERROR",model,"run_identity",
                "One source bundle contains records from more than one cycle.",run_time_values=run_values))
        if model_identity_failures:
            issues.append(issue("MODEL_RECORD_IDENTITY_MISMATCH","ERROR",model,"record_identity",
                "One or more records are stored under a model key that does not match their declared model identity.",
                affected_records=model_identity_failures))
        if invalid_lead_rows:
            issues.append(issue("FORECAST_LEAD_VALUE_INVALID","ERROR",model,"coverage",
                "Some forecast records cannot be assigned to a lead time.",rows=invalid_lead_rows))
        if duplicates:
            issues.append(issue("DUPLICATE_FORECAST_LEADS","ERROR",model,"coverage",
                "More than one record exists for the same model lead.",duplicate_leads_hours=sorted(set(duplicates))))
        if missing:
            issues.append(issue("EXPECTED_PROVIDER_LEADS_NOT_RECEIVED","ERROR",model,"coverage",
                "The selected model cycle does not contain all leads that should be collected from that cycle.",
                expected_leads_hours=expected,received_leads_hours=got,missing_leads_hours=missing,
                selected_cycle_hour_utc=run_hour,provider_expected_max_horizon_hours=pmax))
        if project_gap:
            issues.append(issue("PROJECT_LONG_RANGE_LEADS_NOT_PROVIDED_BY_SELECTED_CYCLE","INFO",model,"coverage",
                "These project-desired long-range leads are outside the documented horizon of the selected provider cycle; this is not a download failure.",
                selected_cycle_hour_utc=run_hour,provider_expected_max_horizon_hours=pmax,
                project_desired_but_cycle_unavailable_leads_hours=project_gap))
        if extra:
            issues.append(issue("RECORDS_OUTSIDE_SELECTED_PROVIDER_CYCLE_HORIZON","INFO",model,"coverage",
                "Records or placeholders outside the selected cycle's documented horizon are excluded from completeness and field-failure gates.",
                provider_expected_max_horizon_hours=pmax,extra_received_leads_hours=extra))
        if field_failures:
            issues.append(issue("REQUIRED_DERIVED_FIELDS_UNAVAILABLE","ERROR",model,"fields",
                "Wind data required by the private integrity gate could not be derived for specific leads.",
                affected_leads=field_failures))
        if coordinate_failures:
            issues.append(issue("MODEL_FORECAST_GRID_IDENTITY_INVALID","ERROR",model,"grid_identity",
                "The actual forecast extraction/grid point is missing, implausible or changes between required leads.",
                canonical_requested_spot=expected_spot,failures=coordinate_failures))
        if timestamp_failures:
            issues.append(issue("FORECAST_TIMESTAMP_OR_LEAD_INCONSISTENCY","ERROR",model,"timestamps",
                "Declared run/valid/lead metadata are internally inconsistent for specific records.",
                affected_records=timestamp_failures))
        if critical_source_errors or model_qerrors:
            issues.append(issue("SOURCE_FETCH_OR_DECODE_ERRORS_RECORDED","ERROR",model,"provider_fetch",
                "Wind-critical provider requests, GRIB extraction or field derivation recorded explicit errors.",
                record_errors=critical_source_errors,quality_errors=model_qerrors))
        if optional_source_warnings:
            issues.append(issue("OPTIONAL_WEATHER_CONTEXT_FIELDS_UNAVAILABLE","WARN",model,"optional_weather_context",
                "Optional precipitation/CAPE context is incomplete at specific leads; wind-core completeness is unaffected.",
                affected_fields=optional_source_warnings))
        if identity_failures:
            issues.append(issue("ENSEMBLE_RUN_IDENTITY_UNVERIFIED","ERROR",model,"run_identity",
                "ICON-D2-EPS run identity could not be proven from stable Open-Meteo metadata plus DWD cycle confirmation.",
                failures=identity_failures))
        if member_failures:
            issues.append(issue("ENSEMBLE_MEMBER_SET_INCOMPLETE","ERROR",model,"ensemble_members",
                "ICON-D2-EPS must contain exactly the fixed 20 member identities at every required lead.",
                affected_leads=member_failures))
        if hourly_source_failures:
            issues.append(issue("ENSEMBLE_HOURLY_SOURCE_INVALID_FOR_V15","ERROR",model,"hourly_ensemble_source",
                "The production bundle does not contain one complete, temporally aligned 20-member hourly ICON-D2-EPS source for v15.",
                failures=hourly_source_failures))
        provider_attempts=[x for x in all_attempts if x.get("model")==model]
        attempt_stages=sorted({str(x.get("stage") or "unknown") for x in provider_attempts})
        for stage in attempt_stages:
            stage_attempts=[x for x in provider_attempts if str(x.get("stage") or "unknown")==stage]
            failed_attempts=[x for x in stage_attempts if x.get("status") in ("failed","failed_external")]
            successful_attempts=[x for x in stage_attempts if x.get("status")=="success"]
            if failed_attempts and successful_attempts:
                issues.append(issue("PROVIDER_RETRY_RECOVERED","WARN",model,"provider_attempts",
                    "One or more provider attempts failed, but a later attempt for the same acquisition stage succeeded.",
                    stage=stage,attempts=stage_attempts))
            elif failed_attempts and not successful_attempts:
                issues.append(issue("PROVIDER_STAGE_ATTEMPTS_FAILED","ERROR",model,"provider_attempts",
                    "All recorded attempts for a requested acquisition stage failed; success in another stage does not mask this failure.",
                    stage=stage,attempts=stage_attempts))
        run_age=None;age_limit=float(cfg["model_policy"]["maximum_run_age_hours"][model])
        mature_archive_exception=False;mature_archive_evidence=None
        if run:
            run_age=(now-run).total_seconds()/3600
            future_tol=float(cfg["model_policy"]["run_timestamp_future_tolerance_minutes"])/60
            if run_age < -future_tol:
                issues.append(issue("MODEL_RUN_TIME_IMPLAUSIBLY_FUTURE","ERROR",model,"currentness",
                    "The selected model cycle is timestamped too far in the future.",
                    run_time_utc=run.isoformat(),checked_at_utc=now.isoformat(),run_age_hours=round(run_age,3),
                    allowed_future_hours=round(future_tol,3)))
            elif run_age>age_limit:
                if model=="GEFS-control":
                    mature_archive_exception,mature_archive_evidence=gefs_mature_cycle_archive_exception(d,run,now)
                elif model=="ICON-EU":
                    mature_archive_exception,mature_archive_evidence=icon_eu_mature_cycle_archive_exception(d,run,now)
                if mature_archive_exception:
                    if model=="GEFS-control":
                        code="GEFS_NEWEST_MATURE_CYCLE_EXCEEDS_NOMINAL_CURRENTNESS"
                        message="The GEFS cycle exceeds nominal forecast freshness but is proven to be the newest cycle whose cycle-specific full native horizon is published. It is retained for full-horizon archive validation but does not count as current forecast-family evidence."
                    else:
                        code="ICON_EU_NEWEST_MATURE_CYCLE_EXCEEDS_NOMINAL_CURRENTNESS"
                        message="The ICON-EU cycle exceeds nominal forecast freshness but is proven to be the newest main cycle with the complete 120 h wind-critical horizon published. It is retained for deliberate full-horizon validation but does not count as current forecast-family evidence."
                    issues.append(issue(code,"WARN",model,"currentness",message,
                        run_time_utc=run.isoformat(),checked_at_utc=now.isoformat(),run_age_hours=round(run_age,3),
                        maximum_run_age_hours=age_limit,excess_age_hours=round(run_age-age_limit,3),
                        mature_cycle_selection=mature_archive_evidence))
                else:
                    issues.append(issue("MODEL_RUN_OLDER_THAN_CURRENTNESS_POLICY","ERROR",model,"currentness",
                        "A newer provider cycle should normally be available; the exact excess age is recorded.",
                        run_time_utc=run.isoformat(),checked_at_utc=now.isoformat(),run_age_hours=round(run_age,3),
                        maximum_run_age_hours=age_limit,excess_age_hours=round(run_age-age_limit,3),
                        mature_cycle_exception_validation=mature_archive_evidence))

        sources[model]={
            "family":FAMILY[model],"record_count":len(recs),
            "selected_run_time_utc":run.isoformat() if run else None,"selected_cycle_hour_utc":run_hour,
            "run_age_hours":round(run_age,3) if run_age is not None else None,"maximum_run_age_hours":age_limit,
            "currentness_policy_mode":"nominal_age_gate_with_full_validation_maturity_exception",
            "gefs_mature_archive_exception_applied":bool(mature_archive_exception) if model=="GEFS-control" else False,
            "gefs_mature_archive_evidence":mature_archive_evidence if model=="GEFS-control" else None,
            "icon_eu_mature_archive_exception_applied":bool(mature_archive_exception) if model=="ICON-EU" else False,
            "icon_eu_mature_archive_evidence":mature_archive_evidence if model=="ICON-EU" else None,
            "provider_expected_max_horizon_hours":pmax,"project_desired_max_horizon_hours":max(desired_leads(model,cfg)),
            "expected_collection_leads_hours":expected,"received_leads_hours":got,"missing_expected_leads_hours":missing,
            "extra_received_leads_hours":extra,"project_desired_but_cycle_unavailable_leads_hours":project_gap,
            "duplicate_leads_hours":sorted(set(duplicates)),"required_field_failures":field_failures,
            "forecast_coordinate_or_grid_point":coordinate_summary,
            "forecast_grid_identity_failures":coordinate_failures,
            "timestamp_failures":timestamp_failures,"provider_or_decode_errors":critical_source_errors,
            "provider_attempts":provider_attempts,
            "ensemble_run_identity":identity_summary,
            "ensemble_member_count_by_lead":member_count_by_lead,
            "ensemble_run_identity_failures":identity_failures,
            "ensemble_member_failures":member_failures,
            "ensemble_hourly_source":hourly_source_summary,
            "ensemble_hourly_source_failures":hourly_source_failures,
            "optional_weather_context_warnings":optional_source_warnings,
            "out_of_horizon_records":outside_horizon_records,
            "quality_error_messages":model_qerrors,
            "provider_cycle_complete":not any([missing,duplicates,invalid_lead_rows,model_identity_failures,field_failures,coordinate_failures,timestamp_failures,critical_source_errors,model_qerrors,identity_failures,member_failures,hourly_source_failures]) and run is not None,
            "currentness_policy_pass":run_age is not None and run_age<=age_limit and run_age>=-float(cfg["model_policy"]["run_timestamp_future_tolerance_minutes"])/60,
        }

    gefs_full_failures,gefs_full_summary,gefs_full_attempt=audit_gefs_full_member_source(d,expected_spot)
    if gefs_full_failures:
        issues.append(issue("GEFS_FULL_MEMBER_SOURCE_INVALID","ERROR","NOAA_GEFS","ensemble_archive",
            "The optional full-member source is present but violates the frozen sparse 2F-3 contract.",
            failures=gefs_full_failures))
    if gefs_full_summary and (not gefs_full_summary.get("network_budget_pass") or not gefs_full_summary.get("request_budget_pass")):
        issues.append(issue("GEFS_FULL_MEMBER_TRAFFIC_BUDGET_EXCEEDED","WARN","NOAA_GEFS","traffic_budget",
            "The complete source is structurally valid but exceeded a frozen 2F-3 daily traffic target.",
            request_metrics=gefs_full_summary.get("request_metrics"),
            traffic_metrics=gefs_full_summary.get("traffic_metrics"),
            network_budget_bytes=gefs_full_summary.get("network_budget_bytes"),
            request_budget=gefs_full_summary.get("request_budget")))
    if gefs_full_summary and gefs_full_summary.get("provider_summary_qa_status")=="unavailable":
        issues.append(issue("GEFS_PROVIDER_SUMMARY_QA_UNAVAILABLE","WARN","NOAA_GEFS","ensemble_summary_qa",
            "Full member values remain authoritative, but the optional NOAA geavg/gespr QA comparison was unavailable for this cycle.",
            qa=gefs_full_summary.get("provider_summary_qa")))
    if isinstance(gefs_full_attempt,dict):
        issues.append(issue("GEFS_FULL_MEMBER_ATTEMPT_INCOMPLETE","WARN","NOAA_GEFS","ensemble_archive",
            "A new optional full-member attempt was incomplete; the last complete member source remains authoritative.",
            attempt=gefs_full_attempt))

    complete_current_families=sorted({v["family"] for v in sources.values() if v["provider_cycle_complete"] and v["currentness_policy_pass"]})
    usable=len(complete_current_families)>=2
    if not usable:
        issues.append(issue("FEWER_THAN_TWO_COMPLETE_CURRENT_INDEPENDENT_MODEL_FAMILIES","ERROR","collector","family_gate",
            "The private forecast must not treat this acquisition as sufficient multi-family evidence.",
            complete_current_families=complete_current_families,required_count=2,observed_count=len(complete_current_families)))
    archive_summary = None
    if "full_horizon_archive" in d:
        from mardorf_collector.contracts.full_horizon_contract import validate_archive
        try:
            archive_summary = validate_archive(d)
            coverage_issue = full_horizon_coverage_issue(
                archive_summary, os.getenv("FULL_VALIDATION", "").lower() == "true")
            if coverage_issue:
                code, severity, impact = coverage_issue
                issues.append(issue(code, severity, "archive", "coverage", impact, coverage=archive_summary))
        except Exception as exc:
            issues.append(issue("FULL_HORIZON_ARCHIVE_INVALID", "ERROR", "archive", "integrity",
                "Archive identities failed revalidation.", reason=str(exc)))
            usable = False
    # Native EPS source admission is independent of the retired API/v15 route.
    # A source-complete native archive may not silently promote that old runtime.
    native_only=bool(d.get('native_eps_source'))
    report=make_report("models",now,sources,issues,usable,{
        "input_file_present":True,"collector_mode":mode,"full_horizon_archive":archive_summary,
        "gefs_full_member_source":gefs_full_summary,
        "gefs_full_member_attempt":gefs_full_attempt,
        "retrieved_at_utc":retrieval.isoformat() if retrieval else None,
        "complete_current_independent_families":complete_current_families,
        "minimum_two_complete_current_independent_families_met":usable,
        **input_meta,
    })
    if native_only:
        report['native_acquisition_ready']=report['error_count']==0 and usable
        report['legacy_v15_compatible']=False
        report['bundle_ready_for_private_revalidation']=False
    return report

def audit_svg(path,cfg,now):
    issues=[];sources={}
    if not path.exists():
        issues.append(issue("SVG_BUNDLE_FILE_NOT_CREATED","ERROR","SVG-42374","bundle",
            "No SVG payload exists to transfer or ingest.",path=str(path)))
        return make_report("svg",now,sources,issues,False,{"input_file_present":False})
    try:raw=path.read_bytes()
    except Exception as e:
        issues.append(issue("SVG_BUNDLE_FILE_READ_FAILED","ERROR","SVG-42374","bundle",
            "The SVG payload exists but cannot be read.",exception_type=type(e).__name__,exception_message=str(e),path=str(path)))
        return make_report("svg",now,sources,issues,False,{"input_file_present":True})
    input_meta={"input_payload_sha256":hashlib.sha256(raw).hexdigest(),"input_payload_bytes":len(raw)}
    try:d=json.loads(raw.decode("utf-8"))
    except Exception as e:
        issues.append(issue("SVG_BUNDLE_JSON_INVALID","ERROR","SVG-42374","bundle",
            "The SVG payload cannot be parsed.",exception_type=type(e).__name__,exception_message=str(e),path=str(path)))
        return make_report("svg",now,sources,issues,False,{"input_file_present":True,**input_meta})
    station=d.get("station") if isinstance(d.get("station"),dict) else {}
    if station.get("id")!=cfg["svg_policy"]["station_id"]:
        issues.append(issue("SVG_STATION_ID_MISMATCH","ERROR","SVG-42374","station_identity",
            "The WeatherLink payload station identity does not match the configured operational SVG station.",
            observed_station=station,expected_station_id=cfg["svg_policy"]["station_id"]))

    reqs=d.get("request_diagnostics") or {}
    for endpoint in ("current","historic","stations"):
        x=reqs.get(endpoint)
        sev="ERROR" if endpoint!="stations" else "WARN"
        if not isinstance(x,dict):
            issues.append(issue("SVG_REQUEST_DIAGNOSTIC_NOT_RECORDED",sev,"SVG-42374",endpoint,
                "The collector cannot prove how this WeatherLink request completed.",endpoint=endpoint))
        elif not x.get("success"):
            issues.append(issue("WEATHERLINK_REQUEST_FAILED",sev,"SVG-42374",endpoint,
                "A WeatherLink request failed; exact HTTP/exception information is attached.",
                endpoint=endpoint,http_status=x.get("http_status"),exception_type=x.get("exception_type"),
                exception_message=x.get("exception_message"),request_path=x.get("request_path")))

    obs=d.get("latest_observation") if isinstance(d.get("latest_observation"),dict) else None
    ot=dt(obs.get("time_utc")) if obs else None;age=None
    if ot:
        age=(now-ot).total_seconds()/60
        future_tol=float(cfg["svg_policy"]["maximum_timestamp_future_tolerance_minutes"])
        fresh=float(cfg["svg_policy"]["fresh_target_minutes"]);maxage=float(cfg["svg_policy"]["maximum_current_state_age_minutes"])
        if age < -future_tol:
            issues.append(issue("SVG_OBSERVATION_TIMESTAMP_TOO_FAR_IN_FUTURE","ERROR","SVG-42374","current_observation",
                "Current observation chronology is implausible.",observation_time_utc=ot.isoformat(),
                checked_at_utc=now.isoformat(),future_offset_minutes=round(-age,2),allowed_future_minutes=future_tol))
        elif age>maxage:
            issues.append(issue("SVG_CURRENT_OBSERVATION_EXCEEDS_MAXIMUM_AGE","ERROR","SVG-42374","current_observation",
                "The current observation is too old for current-state use.",observation_time_utc=ot.isoformat(),
                checked_at_utc=now.isoformat(),observation_age_minutes=round(age,2),
                maximum_current_state_age_minutes=maxage,excess_age_minutes=round(age-maxage,2)))
        elif age>fresh:
            issues.append(issue("SVG_CURRENT_OBSERVATION_EXCEEDS_FRESH_TARGET","WARN","SVG-42374","current_observation",
                "The observation remains within the maximum current-state age but is older than the preferred freshness target.",
                observation_time_utc=ot.isoformat(),checked_at_utc=now.isoformat(),observation_age_minutes=round(age,2),
                fresh_target_minutes=fresh,excess_over_fresh_target_minutes=round(age-fresh,2)))
    else:
        issues.append(issue("SVG_CURRENT_OBSERVATION_TIMESTAMP_UNAVAILABLE","ERROR","SVG-42374","current_observation",
            "No parseable current observation timestamp is present.",value=(obs or {}).get("time_utc")))

    rows=[x for x in (d.get("recent_historic_observations") or []) if isinstance(x,dict)]
    parsed=[dt(x.get("time_utc")) for x in rows]
    valid_times=[x for x in parsed if x is not None]
    times=sorted(set(valid_times))
    invalid_history_times=[{"index":i,"value":rows[i].get("time_utc")} for i,x in enumerate(parsed) if x is None]
    duplicates=len(valid_times)-len(times);cadence=float(cfg["svg_policy"]["archive_expected_cadence_minutes"]);gaps=[]
    for a,b in zip(times,times[1:]):
        diff=(b-a).total_seconds()/60
        if diff>cadence+0.1:
            gaps.append({"after_utc":a.isoformat(),"before_utc":b.isoformat(),"gap_minutes":round(diff,2),
                         "estimated_missing_five_minute_intervals":max(0,int(round(diff/cadence))-1)})

    requested=d.get("requested_history_window") or {}
    requested_start=dt(requested.get("start_utc"));requested_end=dt(requested.get("end_utc"))
    future_tol=float(cfg["svg_policy"]["maximum_timestamp_future_tolerance_minutes"])
    future_history_times=[x.isoformat() for x in valid_times if x>now+timedelta(minutes=future_tol)]
    outside_window_times=[]
    if requested_start and requested_end:
        outside_window_times=[x.isoformat() for x in valid_times if x<requested_start or x>requested_end]
    expected_slots=None;start_gap=None;end_gap=None;coverage_ratio=None
    if requested_start and requested_end and requested_end>requested_start:
        expected_slots=max(1,int((requested_end-requested_start).total_seconds()//(cadence*60)))
        if times:
            start_gap=max(0.0,(times[0]-requested_start).total_seconds()/60)
            end_gap=max(0.0,(requested_end-times[-1]).total_seconds()/60)
            coverage_ratio=min(1.0,len(times)/expected_slots) if expected_slots else None

    if not rows:
        issues.append(issue("SVG_HISTORIC_WINDOW_RETURNED_ZERO_RECORDS","ERROR","SVG-42374","historic_window",
            "The routine WeatherLink history request returned no normalized archive records.",
            requested_history_hours=cfg["svg_policy"]["routine_history_hours"]))
    if invalid_history_times:
        issues.append(issue("SVG_HISTORIC_TIMESTAMP_INVALID","ERROR","SVG-42374","historic_window",
            "One or more WeatherLink archive rows contain an unparseable timestamp.",
            affected_rows=invalid_history_times))
    if future_history_times:
        issues.append(issue("SVG_HISTORIC_TIMESTAMP_TOO_FAR_IN_FUTURE","ERROR","SVG-42374","historic_window",
            "One or more WeatherLink archive timestamps are implausibly in the future.",
            affected_times_utc=future_history_times,allowed_future_minutes=future_tol))
    if outside_window_times:
        issues.append(issue("SVG_HISTORIC_TIMESTAMP_OUTSIDE_REQUEST_WINDOW","ERROR","SVG-42374","historic_window",
            "One or more WeatherLink archive timestamps fall outside the requested history window.",
            affected_times_utc=outside_window_times,
            requested_start_utc=requested_start.isoformat() if requested_start else None,
            requested_end_utc=requested_end.isoformat() if requested_end else None))
    if duplicates:
        issues.append(issue("SVG_HISTORIC_DUPLICATE_TIMESTAMPS","WARN","SVG-42374","historic_window",
            "Repeated archive timestamps were present in the normalized history.",duplicate_record_count=duplicates))
    if gaps:
        issues.append(issue("SVG_HISTORIC_CADENCE_GAPS","WARN","SVG-42374","historic_window",
            "The recent archive has gaps larger than the expected five-minute cadence; affected intervals are explicit.",
            expected_cadence_minutes=cadence,gaps=gaps,
            total_estimated_missing_intervals=sum(x["estimated_missing_five_minute_intervals"] for x in gaps)))
    edge_tol=float(cfg["svg_policy"]["archive_edge_tolerance_minutes"])
    if start_gap is not None and start_gap>edge_tol:
        issues.append(issue("SVG_HISTORIC_WINDOW_START_NOT_COVERED","WARN","SVG-42374","historic_window",
            "The first returned archive record begins later than the requested history window.",
            requested_start_utc=requested_start.isoformat(),first_record_utc=times[0].isoformat(),
            uncovered_start_minutes=round(start_gap,2),allowed_edge_tolerance_minutes=edge_tol))
    if end_gap is not None and end_gap>edge_tol:
        issues.append(issue("SVG_HISTORIC_WINDOW_END_NOT_COVERED","WARN","SVG-42374","historic_window",
            "The last returned archive record ends too far before the requested history-window end.",
            requested_end_utc=requested_end.isoformat(),last_record_utc=times[-1].isoformat(),
            uncovered_end_minutes=round(end_gap,2),allowed_edge_tolerance_minutes=edge_tol))

    current_req_ok=bool((reqs.get("current") or {}).get("success"))
    historic_req_ok=bool((reqs.get("historic") or {}).get("success"))
    within_age=age is not None and age<=float(cfg["svg_policy"]["maximum_current_state_age_minutes"]) and age>=-float(cfg["svg_policy"]["maximum_timestamp_future_tolerance_minutes"])
    historic_time_integrity_ok=not invalid_history_times and not future_history_times and not outside_window_times
    usable=current_req_ok and historic_req_ok and within_age and ot is not None and bool(rows) and historic_time_integrity_ok
    sources["SVG-42374"]={
        "current_request":reqs.get("current"),"historic_request":reqs.get("historic"),"metadata_request":reqs.get("stations"),
        "current_observation_time_utc":ot.isoformat() if ot else None,
        "current_observation_age_minutes":round(age,2) if age is not None else None,
        "fresh_target_minutes":cfg["svg_policy"]["fresh_target_minutes"],
        "maximum_current_state_age_minutes":cfg["svg_policy"]["maximum_current_state_age_minutes"],
        "current_state_age_policy_pass":within_age,"historic_record_count":len(rows),
        "historic_unique_timestamp_count":len(times),"historic_first_time_utc":times[0].isoformat() if times else None,
        "historic_last_time_utc":times[-1].isoformat() if times else None,
        "expected_archive_cadence_minutes":cadence,"historic_gap_count":len(gaps),"historic_gaps":gaps,
        "duplicate_historic_record_count":duplicates,
        "requested_history_start_utc":requested_start.isoformat() if requested_start else None,
        "requested_history_end_utc":requested_end.isoformat() if requested_end else None,
        "expected_five_minute_intervals":expected_slots,
        "observed_unique_intervals":len(times),
        "coverage_ratio_of_requested_interval_count":round(coverage_ratio,4) if coverage_ratio is not None else None,
        "uncovered_start_minutes":round(start_gap,2) if start_gap is not None else None,
        "uncovered_end_minutes":round(end_gap,2) if end_gap is not None else None,
    }
    return make_report("svg",now,sources,issues,usable,{
        "input_file_present":True,"retrieved_at_utc":d.get("retrieved_at_utc"),
        **input_meta,
    })

def audit_skm(path,cfg,now):
    issues=[];sources={}
    station="SKM-898";pcfg=cfg.get("skm_policy") or {}
    if not path.exists():
        issues.append(issue("SKM_BUNDLE_FILE_NOT_CREATED","ERROR",station,"bundle",
            "The optional MeteoMap probe produced no payload.",path=str(path)))
        return make_report("skm",now,sources,issues,False,{"input_file_present":False,"non_blocking":True})
    try:raw=path.read_bytes()
    except Exception as e:
        issues.append(issue("SKM_BUNDLE_FILE_READ_FAILED","ERROR",station,"bundle",
            "The optional SKM payload exists but cannot be read.",exception_type=type(e).__name__,exception_message=str(e),path=str(path)))
        return make_report("skm",now,sources,issues,False,{"input_file_present":True,"non_blocking":True})
    input_meta={"input_payload_sha256":hashlib.sha256(raw).hexdigest(),"input_payload_bytes":len(raw)}
    try:d=json.loads(raw.decode("utf-8"))
    except Exception as e:
        issues.append(issue("SKM_BUNDLE_JSON_INVALID","ERROR",station,"bundle",
            "The optional SKM payload cannot be parsed.",exception_type=type(e).__name__,exception_message=str(e),path=str(path)))
        return make_report("skm",now,sources,issues,False,{"input_file_present":True,"non_blocking":True,**input_meta})
    skm_station=d.get("station") if isinstance(d.get("station"),dict) else {}
    if skm_station.get("id")!=pcfg.get("station_id"):
        issues.append(issue("SKM_STATION_ID_MISMATCH","ERROR",station,"station_identity",
            "The MeteoMap payload station identity does not match the configured legacy diagnostic station.",
            observed_station=skm_station,expected_station_id=pcfg.get("station_id")))

    if str(d.get("source_timestamp_timezone"))!="UTC":
        issues.append(issue("SKM_TIMESTAMP_SEMANTICS_NOT_UTC","ERROR",station,"timestamps",
            "MeteoMap chart timestamps must be interpreted as timezone-naive UTC.",
            recorded_source_timestamp_timezone=d.get("source_timestamp_timezone")))

    summary=d.get("endpoint_summary") or {};reqs=d.get("requests") or {}
    if not isinstance(reqs,list):reqs=[]
    required=list(pcfg.get("required_endpoints") or ["wind","gust"])
    future_tol=float(pcfg.get("maximum_timestamp_future_tolerance_minutes",15))
    fresh=float(pcfg.get("fresh_target_minutes",30))
    maxage=float(pcfg.get("maximum_diagnostic_age_minutes",120))
    endpoint_times={}
    required_ok=True
    for kind in required:
        s=summary.get(kind) if isinstance(summary.get(kind),dict) else {}
        t=dt(s.get("last_time_utc"));endpoint_times[kind]=t
        if not s.get("success") or t is None:
            required_ok=False
            related=[x for x in reqs if isinstance(x,dict) and x.get("kind")==kind]
            empty_series=bool(s.get("http_requests_succeeded") and s.get("provider_returned_empty_measurement_series"))
            code="SKM_PROVIDER_SERIES_EMPTY_DESPITE_HTTP_SUCCESS" if empty_series else "SKM_REQUIRED_ENDPOINT_UNAVAILABLE"
            impact=("MeteoMap returned HTTP/JSON success but the station's measurement series is actually empty for all bounded source-day attempts."
                    if empty_series else
                    "The optional SKM probe could not establish a parseable non-future observation for a required endpoint.")
            issues.append(issue(code,"ERROR",station,kind,impact,
                endpoint=kind,endpoint_summary=s,requests=related,
                classification="upstream_station_or_provider_empty_series" if empty_series else "request_or_parse_unavailable"))
            continue
        age=(now-t).total_seconds()/60
        if age < -future_tol:
            required_ok=False
            issues.append(issue("SKM_OBSERVATION_TIMESTAMP_TOO_FAR_IN_FUTURE","ERROR",station,kind,
                "The newest SKM timestamp is implausibly in the future.",
                endpoint=kind,observation_time_utc=t.isoformat(),checked_at_utc=now.isoformat(),
                future_offset_minutes=round(-age,2),allowed_future_minutes=future_tol))
        elif age>maxage:
            issues.append(issue("SKM_OBSERVATION_OLDER_THAN_DIAGNOSTIC_TARGET","WARN",station,kind,
                "The endpoint responded, but the newest SKM observation is too old to describe current lake conditions; it remains archival/diagnostic only.",
                endpoint=kind,observation_time_utc=t.isoformat(),checked_at_utc=now.isoformat(),
                observation_age_minutes=round(age,2),maximum_diagnostic_age_minutes=maxage,
                excess_age_minutes=round(age-maxage,2)))
        elif age>fresh:
            issues.append(issue("SKM_OBSERVATION_EXCEEDS_FRESH_TARGET","WARN",station,kind,
                "The newest SKM observation is older than the preferred freshness target.",
                endpoint=kind,observation_time_utc=t.isoformat(),checked_at_utc=now.isoformat(),
                observation_age_minutes=round(age,2),fresh_target_minutes=fresh))
    for x in reqs:
        if not isinstance(x,dict) or x.get("success"):continue
        kind=x.get("kind")
        sev="ERROR" if kind in required and not (summary.get(kind) or {}).get("success") else "WARN"
        issues.append(issue("SKM_HTTP_OR_PARSE_ATTEMPT_FAILED",sev,station,str(kind or "request"),
            "A bounded MeteoMap request attempt failed; this source is optional and cannot block primary SVG/model evaluation.",
            kind=kind,source_day_utc=x.get("source_day_utc"),http_status=x.get("http_status"),
            exception_type=x.get("exception_type"),exception_message=x.get("exception_message"),
            elapsed_seconds=x.get("elapsed_seconds"),final_url=x.get("final_url") or x.get("request_url")))
    wt=endpoint_times.get("wind");gt=endpoint_times.get("gust")
    if wt and gt:
        delta=abs((wt-gt).total_seconds()/60)
        if delta>20:
            issues.append(issue("SKM_WIND_GUST_TIMESTAMP_MISALIGNMENT","WARN",station,"timestamps",
                "Wind and gust newest timestamps are materially different.",
                wind_time_utc=wt.isoformat(),gust_time_utc=gt.isoformat(),absolute_difference_minutes=round(delta,2)))
    sources[station]={
        "provider":d.get("provider"),"role":d.get("role"),"station":d.get("station"),
        "source_timestamp_timezone":d.get("source_timestamp_timezone"),
        "retrieved_at_utc":d.get("retrieved_at_utc"),"endpoint_summary":summary,
        "request_diagnostics":reqs,"required_endpoints":required,
        "required_endpoints_have_parseable_nonfuture_data":required_ok,
        "non_blocking":True,
    }
    return make_report("skm",now,sources,issues,required_ok,{
        "input_file_present":True,"retrieved_at_utc":d.get("retrieved_at_utc"),
        "non_blocking":True,"operational_authority":False,
        "interpretation":"SKM is a legacy north-shore diagnostic only; PASS/FAIL never gates primary SVG/model evaluation.",
        **input_meta,
    })

def markdown(report):
    lines=[
        "# Collector integrity — "+report["kind"],"",
        "- Generated UTC: "+str(report["generated_at_utc"]),
        "- Status: **"+str(report["status"])+"**",
        "- Errors: %s; warnings: %s; info: %s"%(report["error_count"],report["warning_count"],report["info_count"]),
        "- Bundle ready for private revalidation: "+str(report["bundle_ready_for_private_revalidation"]),""
    ]
    if report["kind"]=="models":
        lines += ["| Source | Run UTC | Age h / limit | received / expected leads | cycle horizon | complete | currentness |",
                  "|---|---|---:|---:|---:|---|---|"]
        for name,x in report["sources"].items():
            lines.append("| %s | %s | %s / %s | %s / %s | %s h | %s | %s |"%(
                name,x["selected_run_time_utc"] or "—",x["run_age_hours"],x["maximum_run_age_hours"],
                len(x["received_leads_hours"]),len(x["expected_collection_leads_hours"]),
                x["provider_expected_max_horizon_hours"],x["provider_cycle_complete"],x["currentness_policy_pass"]))
        eps=report["sources"].get("ICON-D2-EPS") or {}
        ident=eps.get("ensemble_run_identity") or {}
        if ident:
            counts=eps.get("ensemble_member_count_by_lead") or {}
            lines += ["","## ICON-D2-EPS identity evidence","",
                "- Verification status: "+str(ident.get("verification_status")),
                "- Open-Meteo model initialisation UTC: "+str(ident.get("run_time_utc")),
                "- Open-Meteo availability UTC: "+str((ident.get("metadata_before") or {}).get("last_run_availability_time_utc")),
                "- Settling age / minimum: %s / %s s."%(ident.get("settling_age_seconds_at_request"),ident.get("minimum_settling_seconds")),
                "- DWD exact-cycle confirmation: "+str(ident.get("dwd_cycle_confirmation_url")),
                "- Member count by required lead: "+json.dumps(counts,sort_keys=True),
                "- Member identity/completeness failures: %s / %s."%(len(eps.get("ensemble_run_identity_failures") or []),len(eps.get("ensemble_member_failures") or []))]
    elif report["kind"]=="svg":
        x=report["sources"].get("SVG-42374",{})
        lines += [
            "- Current observation UTC: "+str(x.get("current_observation_time_utc")),
            "- Observation age: %s min; fresh target %s min; maximum current-state age %s min."%(
                x.get("current_observation_age_minutes"),x.get("fresh_target_minutes"),x.get("maximum_current_state_age_minutes")),
            "- Historic records: %s; unique timestamps: %s; expected five-minute intervals: %s; coverage ratio: %s."%(
                x.get("historic_record_count"),x.get("historic_unique_timestamp_count"),x.get("expected_five_minute_intervals"),x.get("coverage_ratio_of_requested_interval_count")),
            "- Window edges not covered: start %s min; end %s min; internal cadence gaps: %s."%(
                x.get("uncovered_start_minutes"),x.get("uncovered_end_minutes"),x.get("historic_gap_count")),""
        ]
    else:
        x=report["sources"].get("SKM-898",{})
        es=x.get("endpoint_summary") or {}
        lines += [
            "- Non-blocking legacy diagnostic: **True**",
            "- Operational authority: **False**",
            "- Wind newest UTC / age: %s / %s min."%(es.get("wind",{}).get("last_time_utc"),es.get("wind",{}).get("observation_age_minutes")),
            "- Gust newest UTC / age: %s / %s min."%(es.get("gust",{}).get("last_time_utc"),es.get("gust",{}).get("observation_age_minutes")),
            "- Required endpoint data present: "+str(x.get("required_endpoints_have_parseable_nonfuture_data")),""
        ]
    lines += ["## Exact diagnostics",""]
    if not report["issues"]:lines.append("- No integrity deviations recorded.")
    else:
        for i,x in enumerate(report["issues"],1):
            lines += [
                "### %d. %s — %s"%(i,x["severity"],x["code"]),
                "- Source: %s; scope: %s"%(x["source"],x["scope"]),
                "- Impact: "+x["impact"],"- Details:"
            ]
            for line in json.dumps(x["details"],indent=2,ensure_ascii=False,sort_keys=True).splitlines():
                lines.append("    "+line)
    return "\n".join(lines)+"\n"

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--kind",required=True,choices=("models","svg","skm"))
    ap.add_argument("--input",required=True);ap.add_argument("--json-out",required=True);ap.add_argument("--md-out",required=True)
    args=ap.parse_args();cfg=json.loads(POLICY.read_text(encoding="utf-8"));now=datetime.now(timezone.utc)
    if args.kind=="models":report=audit_models(Path(args.input),cfg,now)
    elif args.kind=="svg":report=audit_svg(Path(args.input),cfg,now)
    else:report=audit_skm(Path(args.input),cfg,now)
    if args.kind=="models" and Path(args.input).is_file():
        from prep_data01_metadata import attach_metadata
        report=attach_metadata(report,Path(args.input).read_bytes())
    jp=Path(args.json_out);mp=Path(args.md_out);jp.parent.mkdir(parents=True,exist_ok=True);mp.parent.mkdir(parents=True,exist_ok=True)
    jp.write_text(json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    mp.write_text(markdown(report),encoding="utf-8")
    print(json.dumps({
        "status":report["status"],"errors":report["error_count"],"warnings":report["warning_count"],
        "ready":report["bundle_ready_for_private_revalidation"],
        "issues":[{"severity":x["severity"],"code":x["code"],"source":x["source"],"scope":x["scope"],"details":x["details"]} for x in report["issues"]]
    },ensure_ascii=False))

if __name__=="__main__":
    main()
