# Workflow Simplification Plan

A living plan for simplifying the `geoglows_ecflow` workflow. Branched off the
`rapid-to-river-route` work (PR #27), so all references below assume the
river-route codebase, not the RAPID `main`.

## Goals

1. **Reduce complexity** — eliminate duplication, simplify functions, separate
   inputs from logic.
2. **Make implicit explicit** — name constants and configuration, split mixed
   functions, document non-obvious behavior.
3. **Improve maintainability** — make different configurations easy to run and
   add a unit-test safety net.

## Scope

- **In scope:** `geoglows_ecflow/resources/*.py`,
  `geoglows_ecflow/workflow/builders/builder.py`,
  `geoglows_ecflow/workflow/parts/*`, and the `.ecf` task scripts.
- **Out of scope (frozen):** `geoglows_ecflow/workflow/comfies/*`. This is
  vendored ECMWF framework code (Apache 2.0, ~4,700 lines). Refactoring it would
  break future upstream syncs.

## Decisions

- **Tests:** start with unit tests on pure functions only. ecFlow-server /
  suite-definition tests are deferred to a later task.
- **HRES member:** high-resolution is always ensemble member **52**. Keep this
  as a single named constant (not a configurable value). The goal is to replace
  scattered magic `52`/`51`/`53` literals with that one named constant.

---

## Phase 1 — Pure cleanup (no behavior change)

Small, fast, independently reviewable. Builds confidence before touching logic.

- [ ] `builder.py`: fix `Task("dimmy")` typo (should be `"dummy"`) in the
      non-osuite `run_en` branch.
- [ ] `helper_functions.py`: fix `create_logger` — when `log_file` is set, the
      handler's `setLevel` / `setFormatter` / `addHandler` calls live in the
      `else` branch, so file logging never attaches. Move them out so both
      branches configure and register the handler.
- [ ] `builder.py`: remove duplicate imports (`complete`, `Family`, `Task`
      imported multiple times) and the duplicated `nodes` import line.
- [ ] `builder.py`: remove read-but-unused config vars (`with_flood_hazard`,
      `wb_days`, `ens_range`) — or wire them in if they were meant to be used
      (confirm intent first).
- [ ] `builder.py`: fix stale docstring ("GLOFAS suite" → GEOGloWS).
- [ ] `generate_esri_table.py`: fix `int or str` type hint (evaluates to `int`).
- [ ] Standardize the `argparse(nargs=1)` + `args.x[0]` antipattern to plain
      positional args (`day_one_forecast.py`, `netcdf_to_zarr.py`,
      `archive_to_aws.py`).

## Phase 2 — Test harness + CI

The safety net that makes every later refactor safe. Pin **current** behavior.

- [ ] Add `pytest` (+ `pytest-cov`) as a dev dependency in `pyproject.toml`.
- [ ] Create `tests/` with fixtures (tiny synthetic `forecast_run.json`,
      a small return-period table, a minimal `Qout` netCDF).
- [ ] Unit tests for the pure functions:
  - [ ] `helper_functions.get_ensemble_number_from_forecast` (incl. the
        `.205.runoff.grib.runoff.netcdf` special case)
  - [ ] `helper_functions.get_valid_vpucode_list` (3-digit dir filtering)
  - [ ] `helper_functions.get_date_from_forecast_dir`
  - [ ] `run_river_route_forecast._find_state_init` (24/48/72h lookback +
        seasonal fallback)
  - [ ] `prep_river_route_forecast.forecast_preprocess` (manifest shape /
        job keys / HRES-first ordering)
  - [ ] `day_one_forecast.check_for_return_period_flow` and
        `get_time_of_first_exceedance`
  - [ ] `compute_init_flows` time-index selection (`INIT_TIME_INDEX`)
- [ ] Add a GitHub Actions workflow (`.github/workflows/tests.yml`) running the
      suite on the supported Python range (3.11–3.13).

## Phase 3 — Centralize duplication

Guarded by Phase 2 tests.

- [ ] Extract the duplicated dask `config.set({...})` + Blosc/zstd compressor +
      encoding block (currently copy-pasted in `day_one_forecast.py` and
      `netcdf_to_zarr.py`) into one shared zarr-writing helper.
- [ ] Add a small `forecast_run.json` loader to remove the open-and-parse
      boilerplate repeated across `run_river_route_forecast.py`,
      `prep_river_route_forecast.py`, `compute_init_flows.py`,
      `netcdf_to_zarr.py`, and `archive_to_aws.py`.
- [ ] Define return periods `[2, 5, 10, 25, 50, 100]` once and drive the
      currently hand-unrolled ladders from it
      (`day_one_forecast.check_for_return_period_flow` and the two blocks in
      `generate_esri_table.py`).
- [ ] Standardize `logging` setup (formats currently differ per module).

## Phase 4 — Make implicit explicit

Highest value for "make implicit explicit," but touches the most files, so it
goes last.

- [ ] Introduce a single named `HRES_ENSEMBLE_MEMBER = 52` constant and replace
      the scattered literals: `grep -v Qout_..._52.nc` (3× in `nco_calc.ecf`),
      `Qout_*_52.nc` in `netcdf_to_zarr.py`, and `range(1, 53)` /
      `np.arange(1, 52)` / `ensemble=52` in `builder.py` and the scripts.
- [ ] Name the `EMOS_BASE != "12"` gate (repeated 3× in `builder.py`) with a
      meaningful variable.
- [ ] Name the remaining magic numbers: timer offsets (`hours=7`, `hours=9`,
      `"14:15"`), `MEM` values (6000/4000), stream-order threshold (`>= 3`),
      flow-thickness thresholds (20/250/1500/10000/30000), and the 10-day
      windows. (`compute_init_flows.INIT_TIME_INDEX` is the model to follow.)
- [ ] Consolidate the config keys currently read ad hoc via `self.config.get(...)`
      in `builder.py` into one documented place, so what's tunable is visible
      at a glance.

## Follow-ups (later tasks)

- ecFlow-server / suite-definition smoke tests (e.g. building the def in
  `--dry` mode).
- README refresh (carried over from PR #27 review).

---

## Handoff — current status (resume Phase 4 on Ubuntu)

**Done & committed (branch `workflow-simplification`, on fork `JakeGimenes`):**
Phases 1–3 complete; 24 pytest tests pass. Phase 3 added `resources/zarr_io.py`
(shared `write_dataset_to_zarr` + `DASK_ZARR_CONFIG`), `helper_functions`
gained `load_forecast_run`, `RETURN_PERIODS`, and `configure_logging`.

**Why Phase 4 moves to Ubuntu:** on the Windows dev box, `ecflow` is not
pip-installable (no wheel) and `river-route` is not on PyPI, so `builder.py`
can't even be imported and the `.ecf` scripts have no harness. The Ubuntu box
has geoglows-ecflow installed, so the suite definition *can* be built and
tested there.

### Phase 4a — resources/ constants (verifiable; do first)
- `HRES_ENSEMBLE_MEMBER = 52` in `helper_functions.py`. In `netcdf_to_zarr.py`:
  `np.arange(1, HRES_ENSEMBLE_MEMBER)`, `f"Qout_*_{HRES_ENSEMBLE_MEMBER}.nc"`,
  `ensemble=HRES_ENSEMBLE_MEMBER`.
- `THICKNESS_THRESHOLDS = [20, 250, 1500, 10000, 30000]` in
  `generate_esri_table.py`; drive the thickness ladder via `enumerate`
  (levels 2..6). Covered by `tests/test_generate_esri_table.py` — extend it to
  assert thickness too.
- `MIN_STREAM_ORDER = 3` (`day_one_forecast.py:114`),
  `FORECAST_WINDOW_DAYS = 10` (`generate_esri_table.py` 10-day filter).

### Phase 4b — builder.py + nco_calc.ecf (verify on Ubuntu, expression-preserving)
- `range(1, 53)` → `range(1, HRES_ENSEMBLE_MEMBER + 1)` (builder imports the
  constant cross-package from `resources.helper_functions`).
- `EMOS_BASE != "12"` gate (×3) → helper `is_00z_cycle(node)` (gates the
  full-ensemble build; `"12"` = 12Z cycle, so `!= "12"` = 00Z).
- Timers: `HRES_RUN_OFFSET_HOURS = 7`, `ENS_RUN_OFFSET_HOURS = 9`,
  `BARRIER_DONE_TIME = "14:15"`. MEM: `ENS_TASK_MEM_MB = 6000`,
  `ARCHIVE_QINIT_MEM_MB = 4000`.
- Consolidate `self.config.get(...)` reads into one documented block.
- `nco_calc.ecf` `grep -v ..._52.nc` (×3): wire an ecflow
  `Variable("HRES_MEMBER", HRES_ENSEMBLE_MEMBER)` and reference
  `%HRES_MEMBER%` — DECISION PENDING (vs. leaving `52` + a comment). Highest
  risk: wrong wiring silently changes which member is excluded from the
  ensemble mean.

### To do on Ubuntu (before/with Phase 4b)
1. Pull the deferred suite-definition smoke tests forward: build the def
   in-memory and assert structure (e.g. 00Z cycle builds 52 ensemble tasks,
   `HRES_MEMBER` variable resolves, run timers are +7h/+9h). These guard 4b.
2. Make the `conftest.py` `river_route` shim conditional — only inject the
   dummy when the real import fails — so the real package is used where present.
3. Fix CI: `pip install -e ".[dev]"` cannot resolve `river-route` on PyPI, so
   the workflow will fail at install. Run `pip show river-route ecflow` to find
   their real source, then either point CI at that index or install test-only
   deps. (`ecflow` is also an undeclared dependency — not in `pyproject.toml`.)
