"""Checks for first-class CPU/PIM memory-system modeling and acceptance gates."""

from __future__ import annotations

import copy
import unittest
import warnings

import yaml

import sources
from simulator import generate_runs_from_config, scale_boundaries_exact


class SimulatorChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open("configs/runs.yaml", "r", encoding="utf-8") as handle:
            cls.config = yaml.safe_load(handle)
        cls.metrics, cls.traces = generate_runs_from_config(cls.config)

    def _find_row(
        self,
        dataset_profile: str,
        scenario: str,
        multiplier: float,
        metrics: list[dict] | None = None,
    ) -> dict:
        rows = self.metrics if metrics is None else metrics
        for row in rows:
            if (
                row["dataset_profile"] == dataset_profile
                and row["scenario"] == scenario
                and abs(float(row["stage_size_multiplier"]) - float(multiplier)) < 1e-12
            ):
                return row
        raise KeyError((dataset_profile, scenario, multiplier))

    def _ratio(self, metrics: list[dict], dataset_profile: str, multiplier: float = 1.0) -> float:
        bounce = self._find_row(
            dataset_profile=dataset_profile,
            scenario=sources.SCENARIO_PIM_HOST_BOUNCE,
            multiplier=multiplier,
            metrics=metrics,
        )
        direct = self._find_row(
            dataset_profile=dataset_profile,
            scenario=sources.SCENARIO_PIM_FLOWCXL_DIRECT,
            multiplier=multiplier,
            metrics=metrics,
        )
        return float(bounce["makespan_s"]) / float(direct["makespan_s"])

    def _tpch_profiles(self) -> list[str]:
        return [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
        ]

    def _traces_for(self, run_id: str, *, op_type: str | None = None) -> list[dict]:
        rows = [row for row in self.traces if row["run_id"] == run_id]
        if op_type is not None:
            rows = [row for row in rows if row["op_type"] == op_type]
        return rows

    def test_memory_system_schema_validation_required_keys(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg.pop("memory_system_by_template", None)
        cfg.pop("enable_memory_ceiling_by_template", None)
        with self.assertRaises((KeyError, ValueError)):
            generate_runs_from_config(cfg)

    def test_legacy_memory_keys_are_normalized_with_warning(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg.pop("memory_system_by_template", None)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_CPU_ONLY, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            metrics, traces = generate_runs_from_config(cfg)
        self.assertTrue(metrics)
        self.assertTrue(traces)
        self.assertTrue(any(issubclass(w.category, UserWarning) for w in caught))

    def test_cpu_engine_gates_materialization_vectorized_vs_blocking(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE

        cfg_vec = copy.deepcopy(self.config)
        cfg_vec["dataset_profiles"] = [profile]
        cfg_vec["size_multipliers"] = [1.0]
        cfg_vec["scenarios"] = [sources.SCENARIO_CPU_ONLY]
        cfg_vec["memory_system_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]["cpu_baseline_system"][
            "baseline_engine"
        ] = sources.CPU_ENGINE_VECTORIZED_PIPELINE

        metrics_vec, traces_vec = generate_runs_from_config(cfg_vec)
        row_vec = metrics_vec[0]
        run_id_vec = row_vec["run_id"]
        materialize_vec = [r for r in traces_vec if r["run_id"] == run_id_vec and r["op_type"] == "MATERIALIZE"]
        self.assertFalse(materialize_vec)
        self.assertEqual(int(row_vec["total_cpu_materialize_bytes"]), 0)

        cfg_blk = copy.deepcopy(cfg_vec)
        cfg_blk["memory_system_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]["cpu_baseline_system"][
            "baseline_engine"
        ] = sources.CPU_ENGINE_BLOCKING_VOLCANO

        metrics_blk, traces_blk = generate_runs_from_config(cfg_blk)
        row_blk = metrics_blk[0]
        run_id_blk = row_blk["run_id"]
        materialize_blk = [r for r in traces_blk if r["run_id"] == run_id_blk and r["op_type"] == "MATERIALIZE"]
        self.assertTrue(materialize_blk)

        boundaries = sources.DATASET_PROFILES[profile]["boundaries_bytes"]
        scaled = scale_boundaries_exact(boundaries, 1.0)
        expected_bytes = int(scaled[1] + scaled[2])
        self.assertEqual(int(row_blk["total_cpu_materialize_bytes"]), expected_bytes)

    def test_cpu_memory_service_random_patterns_latency_limited(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        row = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0)
        run_id = row["run_id"]
        compute_rows = self._traces_for(run_id, op_type="COMPUTE")
        join_rows = [r for r in compute_rows if r["stage_name"] == "join" and r["stage_device"] == sources.DEVICE_CPU]
        groupby_rows = [
            r for r in compute_rows if r["stage_name"] == "groupby_agg" and r["stage_device"] == sources.DEVICE_CPU
        ]
        self.assertTrue(join_rows)
        self.assertTrue(groupby_rows)
        self.assertTrue(any(r["cpu_mem_bound_mode"] == "latency_limited" for r in join_rows))
        self.assertTrue(any(r["cpu_mem_bound_mode"] == "latency_limited" for r in groupby_rows))

    def test_cpu_memory_queue_delay_positive_under_pressure(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_CPU_ONLY]
        stage_cfg = cfg["memory_system_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]["cpu_baseline_system"]["stages"]
        stage_cfg["join"]["peak_bw_Bps"] = 8.0e9
        stage_cfg["groupby_agg"]["peak_bw_Bps"] = 8.0e9
        metrics, _ = generate_runs_from_config(cfg)
        row = metrics[0]
        self.assertGreater(float(row["total_cpu_mem_queue_delay_component_s"]), 0.0)
        self.assertGreater(float(row["total_cpu_mem_service_time_component_s"]), 0.0)

    def test_pim_memory_system_path_uses_first_class_config(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        baseline = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0)
        baseline_makespan = float(baseline["makespan_s"])
        baseline_pim_mem = float(baseline["total_pim_mem_time_component_s"])

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [profile]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        pim_stages = cfg["memory_system_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]["pim_system"]["stages"]
        for stage_name in sources.TPCH_STAGE_NAMES:
            pim_stages[stage_name]["peak_bw_Bps"] = 6.0e9
        metrics_slow, _ = generate_runs_from_config(cfg)
        row_slow = metrics_slow[0]

        self.assertGreater(float(row_slow["total_pim_mem_time_component_s"]), 0.0)
        self.assertGreater(float(row_slow["makespan_s"]), baseline_makespan)
        self.assertGreater(float(row_slow["total_pim_mem_time_component_s"]), baseline_pim_mem)

    def test_directional_links_unchanged(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        row = self._find_row(profile, sources.SCENARIO_PIM_HOST_BOUNCE, 1.0)
        run_id = row["run_id"]
        h2d_link = self.config["link_profile"]["host_h2d_link"]
        d2h_link = self.config["link_profile"]["host_d2h_link"]

        transfer_rows = [r for r in self._traces_for(run_id, op_type="TRANSFER")]
        self.assertTrue(transfer_rows)
        h2d_rows = [r for r in transfer_rows if r["transfer_path"] in {"host_h2d_ingress", "host_h2d_stage"}]
        d2h_rows = [r for r in transfer_rows if r["transfer_path"] == "host_d2h"]
        self.assertTrue(h2d_rows)
        self.assertTrue(d2h_rows)
        self.assertTrue(all(r["link_type"] == h2d_link for r in h2d_rows))
        self.assertTrue(all(r["link_type"] == d2h_link for r in d2h_rows))

    def test_direct_not_slower_than_bounce_all_tpch_points(self) -> None:
        for profile in self._tpch_profiles():
            for multiplier in self.config["size_multipliers"]:
                ratio = self._ratio(self.metrics, dataset_profile=profile, multiplier=float(multiplier))
                self.assertGreaterEqual(ratio, 1.0)

    def test_tpch_high_profile_1x_bounce_direct_ratio_at_least_2x(self) -> None:
        ratio = self._ratio(self.metrics, dataset_profile=sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE, multiplier=1.0)
        self.assertGreaterEqual(ratio, 2.0)

    def test_tpch_high_profile_1x_cpu_direct_ratio_at_least_1p2(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        cpu = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0)
        direct = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0)
        ratio = float(cpu["makespan_s"]) / float(direct["makespan_s"])
        self.assertGreaterEqual(ratio, 1.2)

    def test_high_profile_bounce_dominant_lb_is_host_link_or_host_touch(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        bounce = self._find_row(profile, sources.SCENARIO_PIM_HOST_BOUNCE, 1.0)
        self.assertIn(str(bounce["dominant_lb_component"]), {"host_link", "host_touch"})

    def test_deepvariant_default_behavior_unchanged(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_DV_ILLUMINA_WGS_30X]
        cfg["size_multipliers"] = [1.0]
        metrics, traces = generate_runs_from_config(cfg)

        self.assertTrue(metrics)
        for row in metrics:
            self.assertEqual(row["pipeline_template"], sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE)
            self.assertFalse(bool(row["memory_ceiling_enabled"]))
            self.assertEqual(row["cpu_baseline_engine"], sources.CPU_ENGINE_VECTORIZED_PIPELINE)
            self.assertEqual(float(row["total_cpu_mem_service_time_component_s"]), 0.0)
            self.assertEqual(float(row["total_cpu_mem_queue_delay_component_s"]), 0.0)
            self.assertEqual(float(row["total_pim_mem_service_time_component_s"]), 0.0)
            self.assertEqual(float(row["total_pim_mem_queue_delay_component_s"]), 0.0)
            self.assertEqual(int(row["total_cpu_materialize_bytes"]), 0)

        self.assertFalse(any(t["op_type"] == "MATERIALIZE" for t in traces))

    def test_metrics_schema_includes_new_memory_system_columns(self) -> None:
        row = self.metrics[0]
        for key in [
            "cpu_baseline_engine",
            "total_cpu_mem_service_time_component_s",
            "total_cpu_mem_queue_delay_component_s",
            "total_pim_mem_service_time_component_s",
            "total_pim_mem_queue_delay_component_s",
            "memory_ceiling_enabled",
        ]:
            self.assertIn(key, row)

        for forbidden in ["queue_wait_s", "utilization", "speedup_vs_cpu_only"]:
            self.assertNotIn(forbidden, row)


if __name__ == "__main__":
    unittest.main()
