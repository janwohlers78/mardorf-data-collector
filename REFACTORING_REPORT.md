# AUD-20261005-ARCH — public collector result

1. **Executive summary:** retain acquisition/transfer boundaries and frozen source
   releases; improve packaging, CI installation, cache and validation navigation.
2. **Original architecture:** runtime -> providers/WP13/WP15 -> integrity/native
   fields -> storage/transfer. Protected source and historical reproduction retained.
3. **Problems:** P1 hash-bound WP13 core/adapters cycle and large integrity audit;
   P2 missing package metadata, four sequential pip commands, uncached validation,
   missing PR paths for newer Cloud/station locks and tooling.
4. **Target:** same shallow existing packages; explicit Python-3.12 distribution
   metadata and optional features; small current validation facade over immutable
   historical evidence. No new framework/service/abstract interface.
5. **Changes:** pyproject and console entry point, flat-script compatibility;
   combined hash-enforced dependency install, pip cache, complete validation paths,
   concurrency for superseded automatic validation. Manual validation/preparation
   is preserved against automatic cancellation.
6. **Removed complexity:** four installs -> one; current layout-validation entry
   is small and adds no successor-v15 branch to historical code. Historical validator
   bytes remain intact under tests/fixtures/architecture, not discarded as dead code.
7. **Dependencies:** eight external source import roots remain eight; already used
   SDK/native features now explicitly categorized. No runtime library/version removed.
8. **Workflows:** 11 workflows/15 jobs remain 11/15; no acquisition/schedule/transfer
   trigger changed. Only validate.yml setup/filter/concurrency changes are reviewed.
9. **Validation:** baseline **487 tests PASS**; final **487 tests PASS**; editable
   installation, pip check, isolated installed CLI and historical layout validation
   PASS. Linux locks jointly installed successfully before the baseline.
10. **Metrics:** source files 141 -> 141; source LOC 21759 -> 21759; source import
    edges 231 -> 231; SCCs 1 -> 1; exact duplicate function groups 9 -> 9.
    active tool LOC 621 -> 171; workflow LOC 1321 -> 1331. Historical evidence
    copies are retained, so this is not a claim of shrinking total repository size.
    CI installation invocations 4 -> 1 (75% fewer invocation/setup transitions),
    without a claim of measured billed-minute or provider-byte savings.
11. **Remaining debt:** frozen WP13 cycle, large audit_models/collect functions,
    source-pinned historical versions and intentional duplication between format
    releases. Removal requires a qualified successor/private readback contract.
12. **Next:** prioritize a pinned collector-contract successor over cosmetic
    rearrangements; preserve original/causal/readback evidence throughout.

The detailed cross-repository audit and canonical project roadmap are maintained
only in the private companion. See ARCHITECTURE.md and DEPENDENCIES.md here for
public interfaces and responsibilities. No private handoff is copied publicly.
