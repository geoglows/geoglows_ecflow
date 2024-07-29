from geoglows_ecflow.workflow.builders.base import GEOGLOWSBaseBuilder
from geoglows_ecflow.workflow.comfies.ooflow import Trigger, Defuser
from geoglows_ecflow.workflow.comfies.ooflow import all_complete, Event, complete
from geoglows_ecflow.workflow.comfies.ooflow import complete, Limit, InLimit, Variable
from geoglows_ecflow.workflow.comfies.ooflow import RepeatDate, Defstatus
from geoglows_ecflow.workflow.parts.nodes import Family, Task
from geoglows_ecflow.workflow.parts.times import (
    t2t,
    Timer,
    CronDateRefresh,
    CronDataAvailability,
)
from geoglows_ecflow.workflow.comfies.dateandtime import Date, TimeDelta, CalSeq
from geoglows_ecflow.workflow.parts.admin import AdminFamily
from geoglows_ecflow.workflow.parts.epilogs import DummyEpilog
from geoglows_ecflow.workflow.parts.repeats import calseq_repeat
from geoglows_ecflow.workflow.parts.packages import PackageInstallers
from geoglows_ecflow.workflow.comfies.partition import partition


from geoglows_ecflow.workflow.parts.nodes import Family, Task, NominalTime


class Builder(GEOGLOWSBaseBuilder):
    
    comfies_minimum_version = "1.6.2"
    
    ecflow_module = "geoglows_ecflow.workflow.parts.nodes"

    scripts = [
        "geoglows_ecflow/workflow/suites/scripts/rapid",
        "geoglows_ecflow/workflow/suites/scripts/common",
    ]

    includes = [
        "geoglows_ecflow/workflow/suites/scripts/rapid",
        "geoglows_ecflow/workflow/suites/scripts/common",
    ]
    
    def build(self):
        """
        Create parts and wire them together into an GLOFAS suite.
        Naming conventions:
        n_*  -- ecFlow Node object
        e_*  -- ecFlow Event object
        """
        super(Builder, self).build()
        cfg = self.config
        # get suite configuration parameters from the deployment config file
        suite_name = self.config.get("name")
        mode = self.config.get("mode", choices=["prod", "test", "rd"])
        first_date = self.config.get("first_date", type=int)
        last_date = self.config.get("last_date", type=int, default="20300101")
        first_barrier = self.config.get(
            "first_barrier", type=int, default=first_date
        )
        archive_path = self.config.get("exparch")
        suite_dir = self.config.get("workroot")

        with_flood_hazard = self.config.get("with_floodhazard", default=False)
        wb_days = self.config.get("wb_days", type=int, default=10)

        # initially empty suite, provided by parent
        # class will be filled up with content here.
        mars_nworkers = self.config.get("mars_workers", type=int, default=1)
        ens_members = self.config.get("ens_members", type=int, default=51)
        ens_range = self.config.get("ens_range", type=int, default=30)
        suite = self.suite
        par_jobvars = self.jobvars.dest("parallel", fallback="PARENT")

        # Selectable Trigger suites
        o_suite = cfg.get("o_suite", default="/o")
        mc_suite = cfg.get("mc_suite", default="/mc")
        if o_suite[0] != "/":
            o_suite = f"/{o_suite}"
        if mc_suite[0] != "/":
            mc_suite = f"/{mc_suite}"

        # these flags are not user-configurable but
        # depend on other flags

        # admin family
        n_admin = Family("admin")
        n_admin_toggles = Task("toggles")
        n_admin.add(n_admin_toggles)

        e_no_diss = Event("no_diss")
        e_no_web_prod = Event("no_webprod")
        e_no_web_test = Event("no_webtest")
        e_no_ecfs_archive = Event("no_ecfs")
        n_admin_toggles.add(
            e_no_diss,
            e_no_web_prod,
            e_no_web_test,
            e_no_ecfs_archive,
            Trigger("1 == 0"),
            Defuser("1 == 1"),
        )

        with_webpush = False
        with_diss = False
        follow_osuite = False
        in_production = False
        in_test = False

        if mode == "prod":
            in_production = True
        if mode == "test":
            in_test = True

        if in_production or in_test:
            follow_osuite = True

        # make family
        n_make = Family("make")
        n_packages = PackageInstallers(
            packages=["scripts", "rapidpy", "basininflow", "geoglows_ecflow"]
        )

        n_build_petsc = Task("build_petsc")
        if "cc" in self.config.get(
            "jobs.destinations.default.host", default="lxc"
        ):
            n_build_petsc.add_defstatus(complete)
        n_build_rapid = Task("build_rapid")
        n_build_rapid.trigger = n_build_petsc.complete
        n_build_venv = Task("build_venv")
        n_packages.trigger = n_build_venv.complete
        n_statics = Task("install_static_data")
        n_statics.trigger = n_packages.complete
        n_initialize = Task("initialize")
        n_initialize.trigger = n_packages.complete
        calseq_se = CalSeq(monthdays=[1])

        first_ms_date = calseq_se.shift(Date.from_ymd(str(first_date)), -3)
        n_statics.add_variable("YMD", first_ms_date.ymd)

        n_make.add(
            Variable("SMSTRIES", 1),
            n_build_venv,
            n_build_petsc,
            n_build_rapid,
            n_packages,
            n_statics,
            n_initialize,
        )

        n_make.add_inlimit("make")

        vpu_list = self.config.get("vpu_list", type=list, default=[])

        n_make.add(Variable("YMD", first_date))
        suite.add(n_make, n_admin)

        n_barrier = Family("barrier")
        n_barrier_daily = Family("daily")
        barrier_ymd = RepeatDate("YMD", int(first_barrier), int(last_date))
        n_barrier_daily.add(barrier_ymd)

        n_main = Family("main")
        n_main.add(InLimit("main"))
        n_main.trigger = n_make.complete
        n_main.trigger &= n_admin.complete
        main_ymd = calseq_repeat(
            Date.from_ymd(str(first_date)), Date.from_ymd(str(last_date))
        )
        n_main.add(main_ymd)

        n_lag = Family("lag")
        n_lag.trigger = n_make.complete
        lag_ymd = calseq_repeat(
            Date.from_ymd(str(first_date)), Date.from_ymd(str(last_date))
        )
        n_daily_lag = Family("daily")
        n_daily_lag.add(lag_ymd)

        n_barrier.add(n_barrier_daily)

        barrier_00 = NominalTime("00", delta_day=1)
        main_00 = NominalTime("00", delta_day=1)
        lag_00 = NominalTime("00", delta_day=1)

        barrier_12 = NominalTime("12")
        main_12 = NominalTime("12")
        lag_12 = NominalTime("12")

        for barrier_hh, main_hh, lag_hh in (
            (barrier_12, main_12, lag_12),
            (barrier_00, main_00, lag_00),
        ):
            cycle = str(main_hh.time.hh)
            tnom = main_hh.time

            if follow_osuite:
                self.defs.add_extern(f"{mc_suite}/main:YMD")
                self.defs.add_extern(f"{mc_suite}/main/{cycle}/fc0015d/fc")
                self.defs.add_extern(f"{o_suite}/main:YMD")
                self.defs.add_extern(f"{o_suite}/main/{cycle}/fc/model")
                n_run_hr = Family("run_hr").add(
                    Trigger(
                        f"({o_suite}/main:YMD == /{suite_name}/barrier/daily:YMD "
                        f"and {o_suite}/main/{cycle}/fc/model == complete) "
                        f"or ({o_suite}/main:YMD > /{suite_name}/barrier/daily:YMD)"
                    )
                )
                n_run_hr.add(
                    Task("dummy").add(Trigger("0==1")).add(Defuser("1==1"))
                )
                n_run_en = Family("run_en").add(
                    Trigger(
                        f"({mc_suite}/main:YMD == /{suite_name}/barrier/daily:YMD "
                        f"and {mc_suite}/main/{cycle}/fc0015d/fc == complete) "
                        f"or ({mc_suite}/main:YMD > /{suite_name}/barrier/daily:YMD)"
                    )
                )
                n_run_en.add(
                    Task("dummy").add(Trigger("0==1")).add(Defuser("1==1"))
                )

                n_barrier_epilog = Family("last").add(
                    Trigger(
                        f"{o_suite}/main:YMD > /{suite_name}/barrier/daily:YMD"
                    ),
                    Task("sleep").add(Trigger("0==1"), Defuser("1==1")),
                )

            else:
                n_run_hr = Family("run_hr")
                n_run_en = Family("run_en")
                n_run_hr.add(Task("dummy"), Timer(tnom + TimeDelta(hours=7)))
                n_run_en.add(Task("dimmy"), Timer(tnom + TimeDelta(hours=9)))
                n_barrier_epilog = DummyEpilog(done=Timer("14:15"))

            barrier_hh.add(n_run_hr, n_run_en)
            n_barrier_daily.add(barrier_hh)
            n_initialize = Family("initialize")
            n_initialize.trigger = n_run_hr.complete.across("YMD")
            n_initialize.add(Task("initialize"))
            n_initialize.add(Defstatus("complete"))
            n_hr = Family("hres")
            n_hr.trigger = n_run_hr.complete.across("YMD")
            n_hr.trigger &= n_initialize.complete
            n_hr.add(Variable("CONTEXT", "hres"))
            n_ret_hr = Task("retrieve_hres")
            n_hr.add(n_ret_hr)

            n_ens = Family("ens")
            if follow_osuite:
                n_ens.trigger = n_run_en.complete.across("YMD")
            n_ens.trigger &= n_initialize.complete
            n_ens.add(Variable("CONTEXT", "ens"))
            n_ret_ens = Family("retrieve")
            n_ret_ens.add(Variable("NWORKERS", mars_nworkers))
            members = [str(x).zfill(2) for x in range(0, ens_members)]
            mars_members = partition(members, mars_nworkers)

            for worker in range(1, mars_nworkers + 1):
                n_worker = Family(str(worker))
                n_worker.add(Variable("WORKER", worker))
                n_worker.add(
                    Task("retrieve_ens"),
                )
                n_ret_ens.add(n_worker)

            n_prep_ens = Task("prep_task")
            n_prep_ens.trigger = n_ret_ens.complete
            n_prep_ens.trigger &= n_ret_hr.complete
            n_ens_ens = Family("ens_members")
            n_ens_ens.trigger = n_prep_ens.complete
            n_ens_ens.add_variable("MEM", 6000)
            n_ens.add(n_ret_ens)
            if main_hh.get_variable("EMOS_BASE").value() != "12":
                n_ens.add(n_prep_ens, n_ens_ens)

            for vpu in vpu_list:
                # Create the ensemble tasks
                for mem in reversed(range(1, 53)):
                    n_member = Family(f"{vpu}_{mem:02d}").add(
                        Task("ens_member"),
                        Variable("JOB_ID", f"job_{vpu}_{mem}"),
                    )
                    n_ens_ens.add(n_member)

            n_nc_to_zarr = Task("nc_to_zarr")
            n_nc_to_zarr.trigger = n_ens.complete & n_hr.complete

            n_vpus = Family("vpu_list")
            for vpu in vpu_list:
                n_vpu = Family(vpu.replace("-", "_"))
                n_vpu.add_variable("VPU", vpu)
                n_vpu.add_task("plain_table")
                n_vpu.add_task("comp_init")
                n_vpu.add_task("day_one").add_trigger(
                    "plain_table == complete"
                )

                n_vpus.add(n_vpu)
            n_vpus.trigger = n_ens.complete & n_hr.complete

            n_plain_table = Task("combine_plain_table")
            n_plain_table.trigger = n_vpus.complete

            n_forecast_warnings = Task("combine_forecast_warnings")
            n_forecast_warnings.trigger = n_vpus.complete

            n_archive_qinit = Task("archive_qinit")
            n_archive_qinit.add_variable('MEM', 4000)
            n_archive_qinit.add_variable('NCPUS', 12)
            n_archive_qinit.trigger = n_vpus.complete

            n_archive_to_aws = Task("archive_to_aws")
            n_archive_to_aws.trigger = (
                n_ens.complete & n_hr.complete & n_plain_table.complete
            )

            n_diss = Family("diss")
            n_diss_ip = Family("diss_ip")
            n_diss_ip.add(Variable("CONTEXT", "input"))
            n_diss_ip.defuser = e_no_diss
            n_diss_ip.add(Task("diss"))
            n_diss_ip.trigger = n_ret_ens.complete & n_ret_hr.complete
            n_diss.add(n_diss_ip)

            n_diss_fc = Family("diss_fc")
            n_diss_fc.add(Variable("CONTEXT", "rapid"))
            n_diss_fc.defuser = e_no_diss
            n_diss_fc.add(Task("diss"))
            n_diss_fc.trigger = n_nc_to_zarr.complete & n_plain_table.complete & n_forecast_warnings.complete

            if main_hh.get_variable("EMOS_BASE").value() != "12":
                n_diss.add(n_diss_fc)

            n_web = Family("web_push")
            n_web.trigger = n_nc_to_zarr.complete & n_vpus.complete
            n_web_prod = Family("prod")
            n_web_prod.add(Task("web_push"))
            n_web_prod.defuser = e_no_web_prod
            n_web_test = Family("test")
            n_web_test.add(Task("web_push"))
            n_web_test.defuser = e_no_web_test
            n_web.add(n_web_prod, n_web_test)

            if not follow_osuite:
                barrier_ymd = barrier_hh.ymd

            if main_hh.get_variable("EMOS_BASE").value() != "12":
                main_hh.add(
                    n_initialize,
                    n_hr,
                    n_ens,
                    n_nc_to_zarr,
                    n_vpus,
                    n_plain_table,
                    n_forecast_warnings,
                    n_archive_qinit,
                    n_archive_to_aws,
                    n_diss,
                    n_web,
                )
            else:
                main_hh.add(n_hr, n_ens, n_diss)

            n_main.add(main_hh)

            lag_hh.add_task("clean")

            n_lag_arch_init = Task("arch_init")
            n_lag_arch_init.defuser = e_no_ecfs_archive
            n_lag_arch_fc = Task("arch_fc")
            n_lag_arch_fc.defuser = e_no_ecfs_archive
            if main_hh.get_variable("EMOS_BASE").value() != "12":
                lag_hh.add(n_lag_arch_init, n_lag_arch_fc)
            lag_hh.trigger = main_hh.complete.across("YMD")
            n_daily_lag.add(lag_hh)

        n_barrier_daily.add(n_barrier_epilog)
        n_main.add(DummyEpilog(done=barrier_ymd > main_ymd))
        n_lag.add(n_daily_lag)
        n_daily_lag.add(DummyEpilog(done=barrier_ymd > lag_ymd))
        suite.add(n_barrier, n_main, n_lag)
        