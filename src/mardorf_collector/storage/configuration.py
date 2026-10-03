"""Canonical runtime B2 settings; diagnostic predecessor remains frozen in P03."""
import base64
import json
import re
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler,Request,build_opener
from .objects import ObjectError as CanaryError

def b2_settings(environ):
    names = ("B2_ENDPOINT_URL", "B2_REGION", "B2_BUCKET",
             "B2_APPLICATION_KEY_ID", "B2_APPLICATION_KEY")
    missing = [name for name in names if not environ.get(name, "").strip()]
    if missing:
        raise CanaryError("Missing configuration: " + ", ".join(missing))
    settings = {name: environ[name].strip() for name in names}
    # The B2 console presents this host without a scheme. Canonicalize only an
    # exact regional B2 host; never accept arbitrary URLs or downgrade TLS.
    if re.fullmatch(r"s3\.[a-z0-9-]+\.backblazeb2\.com/?", settings["B2_ENDPOINT_URL"]):
        settings["B2_ENDPOINT_URL"] = "https://" + settings["B2_ENDPOINT_URL"]
    url = urlsplit(settings["B2_ENDPOINT_URL"])
    host = re.fullmatch(r"s3\.([a-z0-9-]+)\.backblazeb2\.com", url.hostname or "")
    if (url.scheme != "https" or not host or url.username or url.password or
            url.port not in (None, 443) or url.path not in ("", "/") or
            url.query or url.fragment):
        raise CanaryError("B2_ENDPOINT_URL must be a regional B2 HTTPS S3 endpoint")
    if settings["B2_REGION"] != host[1]:
        raise CanaryError("B2_REGION does not match the S3 endpoint")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{2,62}", settings["B2_BUCKET"]):
        raise CanaryError("Invalid B2_BUCKET name")
    settings["B2_ENDPOINT_URL"] = settings["B2_ENDPOINT_URL"].rstrip("/")
    return settings

def native_json(request):
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self,*args,**kwargs):return None
    with build_opener(NoRedirect).open(request, timeout=10) as response:
        raw = response.read(65537)
    if len(raw) > 65536:
        raise CanaryError("Native B2 metadata exceeds the probe limit")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise CanaryError("Invalid native B2 metadata")
    return data

def resolve_b2_bucket(settings, send=native_json):
    """Resolve an exact configured bucket ID via authenticated provider metadata.

    Tokens stay in memory; no unrestricted bucket listing or guessed name.
    """
    settings = dict(settings)
    configured = settings["B2_BUCKET"]
    if not re.fullmatch(r"[a-f0-9]{24}", configured):
        settings["bucket_name_resolution"] = "configured_name"
        return settings
    basic = base64.b64encode((settings["B2_APPLICATION_KEY_ID"] + ":" +
                             settings["B2_APPLICATION_KEY"]).encode()).decode()
    auth = send(Request("https://api.backblazeb2.com/b2api/v4/b2_authorize_account",
                        headers={"Authorization": "Basic " + basic}))
    api = auth.get("apiInfo", {}).get("storageApi", {})
    buckets = api.get("allowed", {}).get("buckets") or []
    names = [b.get("name") for b in buckets if b.get("id") == configured and b.get("name")]
    if not names:
        url = urlsplit(api.get("apiUrl", ""))
        if (url.scheme != "https" or not re.fullmatch(r"api[0-9]+\.backblazeb2\.com", url.hostname or "") or
                url.username or url.password or url.port not in (None, 443) or
                url.path not in ("", "/") or url.query or url.fragment):
            raise CanaryError("Invalid native B2 metadata endpoint")
        body = json.dumps({"accountId": auth["accountId"], "bucketId": configured}).encode()
        result = send(Request(api["apiUrl"].rstrip("/") + "/b2api/v4/b2_list_buckets", data=body,
                              headers={"Authorization": auth["authorizationToken"],
                                       "Content-Type": "application/json"}))
        names = [b.get("bucketName") for b in result.get("buckets", [])
                 if b.get("bucketId") == configured and b.get("bucketName")]
    if len(names) != 1 or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{2,62}", names[0]):
        raise CanaryError("Configured bucket ID could not be resolved uniquely; use B2_BUCKET name")
    settings["B2_BUCKET"] = names[0]
    settings["bucket_name_resolution"] = "authenticated_native_metadata_exact_id"
    return settings
