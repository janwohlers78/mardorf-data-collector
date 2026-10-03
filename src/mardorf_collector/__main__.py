"""Explicit, lazy collector CLI; subcommands retain existing argument contracts."""
import argparse
import importlib
import sys

COMMANDS = {'wp13-prepare': 'mardorf_collector.wp13.history_v1', 'fetch-model-data': 'mardorf_collector.providers.fetch_model_data', 'fetch-extra-models': 'mardorf_collector.providers.fetch_extra_models', 'fetch-dwd-additional-models': 'mardorf_collector.providers.fetch_dwd_additional_models', 'fetch-svg-weatherlink': 'mardorf_collector.providers.fetch_svg_weatherlink', 'fetch-skm-optional': 'mardorf_collector.providers.fetch_skm_optional', 'fetch-wunstorf': 'mardorf_collector.providers.fetch_wunstorf', 'fetch-etnw-metar': 'mardorf_collector.providers.fetch_etnw_metar', 'provider-fetch': 'mardorf_collector.providers.provider_fetch', 'gefs-full-members': 'mardorf_collector.providers.gefs_full_members', 'icon-parameter-probe': 'mardorf_collector.providers.icon_parameter_probe', 'availability-contract': 'mardorf_collector.contracts.availability_contract', 'audit-integrity': 'mardorf_collector.integrity.audit_integrity', 'audit-secondary-integrity': 'mardorf_collector.integrity.audit_secondary_integrity', 'push-private': 'mardorf_collector.transfer.push_private', 'finalize-secondary-batch': 'mardorf_collector.transfer.finalize_secondary_batch', 'check-collection-due': 'mardorf_collector.runtime.check_collection_due', 'provider-cycle-gate': 'mardorf_collector.runtime.provider_cycle_gate', 'collect-full-horizon': 'mardorf_collector.runtime.collect_full_horizon', 'collect-icon-tier-a': 'mardorf_collector.runtime.collect_icon_tier_a', 'extend-model-horizon': 'mardorf_collector.runtime.extend_model_horizon', 'run-dev03-wp03-i10-public': 'mardorf_collector.runtime.run_dev03_wp03_i10_public', 'measure-gefs-sparse-policy': 'mardorf_collector.runtime.measure_gefs_sparse_policy', 'measure-gefs-summary-qa': 'mardorf_collector.runtime.measure_gefs_summary_qa'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=sorted(COMMANDS))
    args = parser.parse_args(sys.argv[1:2])
    rest = sys.argv[2:]
    module = importlib.import_module(COMMANDS[args.command])
    sys.argv = [module.__file__, *rest]
    return module.main()


if __name__ == "__main__":
    sys.exit(main())
