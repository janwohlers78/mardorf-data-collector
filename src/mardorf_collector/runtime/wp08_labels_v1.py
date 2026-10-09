"""Bounded original-only WP08 labels, captured by the existing public owner."""
import json,hashlib,io,zipfile,csv
from datetime import datetime,timezone
from pathlib import Path,PurePosixPath
import requests

ROOT=Path(__file__).resolve().parents[3]

def validate(raw,station='00662',maximum_expanded=167772160):
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        info=z.infolist()
        if len(info)>64 or sum(i.file_size for i in info)>maximum_expanded or any(PurePosixPath(i.filename).is_absolute() or '..' in PurePosixPath(i.filename).parts or '\\' in i.filename or i.flag_bits&1 for i in info):raise ValueError('WP08_ZIP_budget_or_path')
        names=[i for i in info if i.filename.startswith('produkt_') and i.filename.endswith('.txt')]
        if len(names)!=1:raise ValueError('WP08_unique_original_product')
        with z.open(names[0]) as stream:
            reader=csv.DictReader(io.TextIOWrapper(stream,encoding='latin1'),delimiter=';');columns=[k.strip() for k in reader.fieldnames]
            if not {'STATIONS_ID','MESS_DATUM'}<=set(columns):raise ValueError('WP08_label_columns')
            row=next(reader,None)
            if row is None or {k.strip():v.strip() for k,v in row.items()}['STATIONS_ID'].zfill(5)!=station:raise ValueError('WP08_station')
    return columns

def capture(*,root=ROOT,get=requests.get):
    path=Path(root)/'config/wp08_labels_v1.json';binding=json.loads(path.read_bytes());originals={};records=[]
    for q,url in binding['sources'].items():
        start=datetime.now(timezone.utc).isoformat();raw=b'';status=0;reason=None;columns=[]
        try:
            # Only fixed registered public DWD URLs; no credentials or redirects.
            with get(url,timeout=(8,45),stream=True,allow_redirects=False) as r:
                status=r.status_code;r.raise_for_status()
                for part in r.iter_content(65536):
                    if len(raw)+len(part)>binding['maximum_original_bytes']:raise ValueError('WP08_original_budget')
                    raw+=part
            columns=validate(raw,binding['station_id'],binding['maximum_zip_expanded_bytes'])
        except (requests.RequestException,ValueError,zipfile.BadZipFile) as e:
            reason=str(e) if isinstance(e,ValueError) else type(e).__name__
        capture=datetime.now(timezone.utc).isoformat();sha=hashlib.sha256(raw).hexdigest();originals[q]=raw
        records.append(dict(quantity=q,url=url,station_id=binding['station_id'],started_utc=start,captured_utc=capture,
            available_utc=capture,http_status=status,status='valid' if reason is None else 'invalid',reason=reason,
            source_sha256=sha,source_bytes=len(raw),original_columns=columns))
    return dict(artifact_version='wp08-native-label-receipt-v1',captured_utc=datetime.now(timezone.utc).isoformat(),
        binding_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),records=records,provider_owner='public_collector',all_original_fields_retained=True),originals

def publication(receipt,originals,backend):
    prefix='weather/archive/janwohlers78/mardorf-kitevorhersage/wp08-labels/v1';value=dict(receipt,records=[])
    for r in receipt['records']:
        raw=originals[r['quantity']]
        if hashlib.sha256(raw).hexdigest()!=r['source_sha256'] or len(raw)!=r['source_bytes']:raise ValueError('WP08_capture_integrity')
        ref=backend.put_bytes(prefix+'/originals/'+r['source_sha256'],raw)
        if backend.get_bytes(ref)!=raw:raise ValueError('WP08_label_original_readback')
        value['records'].append(dict(r,original=ref.json()))
    raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode();ref=backend.put_bytes(prefix+'/receipts/'+hashlib.sha256(raw).hexdigest()+'.json',raw)
    if backend.get_bytes(ref)!=raw:raise ValueError('WP08_label_receipt_readback')
    pointer=dict(schema_version=1,artifact_version='wp08-labels-ingress-control-v1',kind='wp08_labels',generated_at_utc=receipt['captured_utc'],snapshot=ref.json(),readback_verified=True,bundle_ready=any(r['status']=='valid' for r in receipt['records']))
    stamp=receipt['captured_utc'].replace(':','')
    return {'data/inbox/wp08_labels/'+stamp+'.json':raw,'data/inbox/wp08_labels/latest.json':raw},pointer
