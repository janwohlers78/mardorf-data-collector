import base64
import hashlib
import json
import unittest
from unittest.mock import patch

import push_private as pp


class FakePatchResponse:
    ok = True
    status_code = 200
    text = ""


class ParentBoundTransferTests(unittest.TestCase):
    def pointer_fixture(self,raw=None):
        raw=raw or (b'{"generated_at_utc":"2026-09-23T10:00:00+00:00"}'+b' '*1100000)
        sha=hashlib.sha1(f"blob {len(raw)}\0".encode()+raw).hexdigest()
        meta={"type":"file","encoding":"none","content":"","sha":sha,"size":len(raw)}
        blob={"encoding":"base64","content":base64.b64encode(raw).decode(),"sha":sha,"size":len(raw)}
        return meta,blob

    def test_large_pointer_blob_is_parent_bound_and_rejects_older_writer(self):
        meta,blob=self.pointer_fixture()
        item={"monotonic_guard":{"path":"latest.json","field":"generated_at_utc",
                                 "incoming_time":"2026-09-23T09:00:00+00:00"}}
        with patch.object(pp,"content_meta",return_value=meta) as cm, \
             patch.object(pp,"req",return_value=blob) as request:
            self.assertFalse(pp.monotonic_allows("owner/private",item,{},ref="fixed-parent"))
        cm.assert_called_once_with("owner/private","latest.json",{},ref="fixed-parent")
        request.assert_called_once_with("GET",f"{pp.API}/repos/owner/private/git/blobs/{meta['sha']}",{})

    def test_large_pointer_corrupt_blob_cannot_bypass_monotonic_guard(self):
        meta,blob=self.pointer_fixture()
        cases=[{**blob,"sha":"0"*40},{**blob,"size":blob["size"]-1},
               {**blob,"encoding":"none"},{**blob,"content":"not base64"},
               {**blob,"content":base64.b64encode(b'x'*blob['size']).decode()}]
        for bad in cases:
            with self.subTest(bad=list(bad)),patch.object(pp,"content_meta",return_value=meta), \
                 patch.object(pp,"req",return_value=bad),self.assertRaises(RuntimeError):
                pp.read_json_pointer("owner/private","latest.json",{},ref="fixed-parent")

    def test_large_pointer_duplicate_keys_remain_rejected(self):
        meta,blob=self.pointer_fixture(b'{"generated_at_utc":"old","generated_at_utc":"new"}'+b' '*1100000)
        with patch.object(pp,"content_meta",return_value=meta),patch.object(pp,"req",return_value=blob), \
             self.assertRaisesRegex(RuntimeError,"pointer cannot be verified"):
            pp.read_json_pointer("owner/private","latest.json",{},ref="fixed-parent")

    def test_absent_and_unreadable_small_pointers_keep_distinct_semantics(self):
        with patch.object(pp,"content_meta",return_value=None),patch.object(pp,"req") as request:
            self.assertIsNone(pp.read_json_pointer("owner/private","latest.json",{}))
            request.assert_not_called()
        with patch.object(pp,"content_meta",return_value={"encoding":"base64","content":""}), \
             patch.object(pp,"req") as request,self.assertRaises(RuntimeError):
            pp.read_json_pointer("owner/private","latest.json",{})
        request.assert_not_called()

    def test_large_pointer_fallback_used_for_descendant_validation(self):
        meta,blob=self.pointer_fixture()
        item={"path":"latest.json","content":b'{}',"immutable":False,
              "monotonic_guard":{"path":"latest.json","field":"generated_at_utc",
                                 "incoming_time":"2026-09-23T09:00:00+00:00"}}
        def response(method,url,h,**kwargs):
            if "/compare/" in url:return {"status":"ahead"}
            if "/git/blobs/" in url:return blob
            raise AssertionError(url)
        with patch.object(pp,"content_meta",return_value=meta) as cm,patch.object(pp,"req",side_effect=response):
            self.assertTrue(pp.descendant_preserves("owner/private","published","new-head",[item],{}))
        self.assertEqual(cm.call_args.kwargs["ref"],"new-head")

    def test_atomic_commit_binds_guards_and_exact_checks_to_parent(self):
        item={
            "path":"data/inbox/public_collector/integrity/models/latest.json",
            "content":b'{"generated_at_utc":"2026-09-23T09:00:00+00:00"}\n',
            "immutable":False,
            "monotonic_guard":{
                "path":"data/inbox/public_collector/integrity/models/latest.json",
                "field":"generated_at_utc",
                "incoming_time":"2026-09-23T09:00:00+00:00",
            },
        }
        calls=[]
        def fake_req(method,url,h,**kwargs):
            calls.append((method,url))
            if method=="GET" and url.endswith("/git/ref/heads/main"):
                return {"object":{"sha":"parent-3" if len([x for x in calls if x[1].endswith('/git/ref/heads/main')])==1 else "commit-new"}}
            if method=="GET" and url.endswith("/git/commits/parent-3"):
                return {"tree":{"sha":"tree-parent"}}
            if method=="POST" and url.endswith("/git/trees"):
                return {"sha":"tree-new"}
            if method=="POST" and url.endswith("/git/commits"):
                return {"sha":"commit-new"}
            raise AssertionError((method,url,kwargs))

        def guard(repo,item,h,ref=None):
            self.assertEqual(ref,"parent-3")
            self.assertTrue(any(u.endswith("/git/ref/heads/main") for _,u in calls))
            return True

        def exact(repo,item,h,ref=None):
            self.assertEqual(ref,"parent-3")
            return False

        with patch.object(pp,"blob",return_value="blob-new"), \
             patch.object(pp,"req",side_effect=fake_req), \
             patch.object(pp,"monotonic_allows",side_effect=guard) as guard_mock, \
             patch.object(pp,"mutable_already_exact",side_effect=exact) as exact_mock, \
             patch.object(pp,"verify_unpublished_commit",return_value=[]), \
             patch.object(pp,"descendant_preserves",return_value=True), \
             patch.object(pp.requests,"patch",return_value=FakePatchResponse()):
            result=pp.atomic_commit("owner/private",[item],"test",{})

        self.assertEqual(result["parent_sha"],"parent-3")
        self.assertEqual(result["commit_sha"],"commit-new")
        self.assertEqual(guard_mock.call_count,1)
        self.assertEqual(exact_mock.call_count,1)

    def test_immutable_collision_check_is_parent_bound(self):
        item={"path":"immutable.json","content":b"x","immutable":True}
        def fake_req(method,url,h,**kwargs):
            if method=="GET" and url.endswith("/git/ref/heads/main"):
                return {"object":{"sha":"parent-fixed"}}
            raise AssertionError((method,url,kwargs))
        def same(repo,path,content,h,gz=False,ref=None):
            self.assertEqual(ref,"parent-fixed")
            return "same-blob"
        with patch.object(pp,"blob",return_value="same-blob"), \
             patch.object(pp,"req",side_effect=fake_req), \
             patch.object(pp,"same_existing",side_effect=same):
            result=pp.atomic_commit("owner/private",[item],"test",{})
        self.assertTrue(result["idempotent"])
        self.assertEqual(result["parent_sha"],"parent-fixed")

    def test_later_descendant_may_advance_monotonic_pointer(self):
        item={
            "path":"latest.json",
            "content":b'{}',
            "immutable":False,
            "monotonic_guard":{
                "path":"latest.json",
                "field":"generated_at_utc",
                "incoming_time":"2026-09-23T09:00:00+00:00",
            },
        }
        newer={"generated_at_utc":"2026-09-23T10:00:00+00:00"}
        meta={"content":base64.b64encode(json.dumps(newer).encode()).decode()}
        with patch.object(pp,"req",return_value={"status":"ahead"}), \
             patch.object(pp,"content_meta",return_value=meta) as cm:
            self.assertTrue(pp.descendant_preserves(
                "owner/private","published","new-head",[item],{}
            ))
        self.assertEqual(cm.call_args.kwargs["ref"],"new-head")

        older={"generated_at_utc":"2026-09-23T08:00:00+00:00"}
        meta={"content":base64.b64encode(json.dumps(older).encode()).decode()}
        with patch.object(pp,"req",return_value={"status":"ahead"}), \
             patch.object(pp,"content_meta",return_value=meta):
            self.assertFalse(pp.descendant_preserves(
                "owner/private","published","new-head",[item],{}
            ))


if __name__=="__main__":
    unittest.main()
