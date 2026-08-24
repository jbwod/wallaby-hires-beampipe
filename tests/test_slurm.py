import os
import signal
import stat
import subprocess
from pathlib import Path

import pytest

from wallaby_hires.slurm import (
    SlurmImagerResources,
    SlurmInterrupted,
    SlurmLifecycleError,
    _parse_job_reference,
    run_setonix_imager,
)


class FakeSlurm:
    def __init__(
        self,
        *,
        job_id="12345;setonix",
        sbatch_stdout=None,
        recovery_ids=(),
        signal_on_queue=False,
        scancel_returncode=0,
        cancel_stays_queued=False,
    ):
        self.job_id = job_id
        self.sbatch_stdout = sbatch_stdout
        self.recovery_ids = recovery_ids
        self.signal_on_queue = signal_on_queue
        self.scancel_returncode = scancel_returncode
        self.cancel_stays_queued = cancel_stays_queued
        self.scancel_called = False
        self.calls = []
        self.call_kwargs = []

    def __call__(self, args, **kwargs):
        command = list(args)
        self.calls.append(command)
        self.call_kwargs.append(kwargs)
        if command[0] == "sbatch":
            stdout = self.sbatch_stdout
            if stdout is None:
                stdout = f"{self.job_id}\n"
            return subprocess.CompletedProcess(command, 0, stdout, "")
        if command[0] == "scontrol":
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[0] == "squeue":
            name_argument = next(
                (item for item in command if item.startswith("--name=")), None
            )
            if name_argument is not None:
                name = name_argument.split("=", 1)[1]
                stdout = "".join(
                    f"{job_id}|{name}|PD|JobHeldUser\n" for job_id in self.recovery_ids
                )
                return subprocess.CompletedProcess(command, 0, stdout, "")
            if self.signal_on_queue:
                self.signal_on_queue = False
                os.kill(os.getpid(), signal.SIGTERM)
                return subprocess.CompletedProcess(command, 0, "12345\n", "")
            if self.scancel_called and self.cancel_stays_queued:
                job_id = self.job_id.split(";", 1)[0]
                return subprocess.CompletedProcess(command, 0, f"{job_id}\n", "")
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[0] == "sacct":
            job_id = next(
                item.split("=", 1)[1] for item in command if item.startswith("--jobs=")
            )
            return subprocess.CompletedProcess(
                command, 0, f"{job_id}|COMPLETED|0:0\n", ""
            )
        if command[0] == "scancel":
            self.scancel_called = True
            return subprocess.CompletedProcess(
                command,
                self.scancel_returncode,
                "",
                "permission denied" if self.scancel_returncode else "",
            )
        raise AssertionError(f"unexpected command: {command}")


def _runtime(tmp_path: Path):
    root = tmp_path / "execution"
    cache = tmp_path / "cache"
    cache.mkdir()
    workdir = root / "source" / "34166" / "beam25"
    workdir.mkdir(parents=True)
    config = tmp_path / "dlg-session" / "imager.in"
    config.parent.mkdir()
    config.write_text(
        "Cimager.dataset = science.ms\n"
        "Cimager.Channels = [250,0]\n"
        "Cimager.nchanpercore = 50\n",
        encoding="utf-8",
    )
    sif = tmp_path / "askapsoft.sif"
    sif.write_bytes(b"sif")
    environment = {
        "BEAMPIPE_SLURM_ACCOUNT": "project123",
        "BEAMPIPE_ASKAPSOFT_SIF": str(sif),
        "WALLABY_HIRES_STAGING_ROOT": str(root),
        "WALLABY_HIRES_CACHE_ROOT": str(cache),
        "DLG_SESSION_ID": "session/unsafe value",
        "SLURM_JOB_ID": "90001",
    }
    return root, workdir, config, environment


def test_nested_imager_records_exact_id_resources_and_terminal_evidence(tmp_path):
    root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm()
    resources = SlurmImagerResources()

    reference = run_setonix_imager(
        workdir,
        config,
        resources=resources,
        environment=environment,
        poll_interval=0,
        run_command=fake,
    )

    assert reference.parsable == "12345;setonix"
    sbatch = fake.calls[0]
    assert "--parsable" in sbatch
    assert "--hold" in sbatch
    assert "--account=project123" in sbatch
    assert "--nodes=1" in sbatch
    assert "--ntasks=6" in sbatch
    assert "--cpus-per-task=1" in sbatch
    assert "--ntasks-per-node=6" in sbatch
    assert "--mem=8G" in sbatch
    assert "--time=00:40:00" in sbatch
    assert [call for call in fake.calls if call[0] == "scontrol"] == [
        ["scontrol", "--clusters=setonix", "release", "12345"]
    ]
    assert not any(call[0] == "scancel" for call in fake.calls)

    lifecycle = next(workdir.glob(".beampipe-imager.*"))
    assert stat.S_IMODE(lifecycle.stat().st_mode) == 0o700
    assert (lifecycle / "child-job-id").read_text() == "12345;setonix\n"
    assert (lifecycle / "child-job-released").read_text() == "12345;setonix\n"
    assert (lifecycle / "child-job-final-state").read_text() == "COMPLETED|0:0\n"
    assert stat.S_IMODE((lifecycle / "child-job-id").stat().st_mode) == 0o600
    assert stat.S_IMODE((lifecycle / "imager.in").stat().st_mode) == 0o600
    script = (lifecycle / "imager-job.sh").read_text()
    assert "srun -N 1 -n 6 -c 1" in script
    assert "srun --exact" not in script
    assert "srun --export" not in script
    assert "srun --ntasks-per-node" not in script
    assert f"HOST_CONFIG={lifecycle / 'imager.in'}" in script
    assert 'ls -lh "${HOST_CONFIG}"' in script
    assert 'ls -lh "${CONFIG}"' not in script
    assert 'echo "SLURM_JOB_CPUS_PER_NODE=${SLURM_JOB_CPUS_PER_NODE:-}"' in script
    assert f"STAGING_ROOT={root}" in script
    assert f"CACHE_ROOT={tmp_path / 'cache'}" in script
    assert "${STAGING_ROOT}:${STAGING_ROOT}" in script
    assert "--pwd /askapbuffer" in script
    assert "/askapbuffer/.beampipe-imager." in script
    assert "PARENT_JOB_ID=90001" in script
    assert 'squeue --noheader --jobs="${PARENT_JOB_ID}"' in script
    assert 'kill -TERM "${imager_pid}"' in script
    assert "parent_lost=1" in script
    syntax = subprocess.run(
        ["bash", "-n", str(lifecycle / "imager-job.sh")],
        check=False,
        capture_output=True,
        text=True,
    )
    assert syntax.returncode == 0, syntax.stderr


def test_nested_imager_strips_parent_slurm_environment_before_sbatch(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    environment.update(
        {
            "PATH": os.environ.get("PATH", ""),
            "SLURM_NTASKS": "2",
            "SLURM_JOB_CPUS_PER_NODE": "2",
            "SLURM_MEM_PER_CPU": "3072",
            "WALLABY_RUNTIME_MARKER": "preserved",
        }
    )
    fake = FakeSlurm()

    run_setonix_imager(
        workdir,
        config,
        environment=environment,
        poll_interval=0,
        run_command=fake,
    )

    sbatch_index = next(
        index for index, call in enumerate(fake.calls) if call[0] == "sbatch"
    )
    submission_environment = fake.call_kwargs[sbatch_index]["env"]
    assert submission_environment["PATH"] == environment["PATH"]
    assert submission_environment["WALLABY_RUNTIME_MARKER"] == "preserved"
    assert not any(key.startswith("SLURM_") for key in submission_environment)


def test_nested_imager_rejects_resources_larger_than_channel_work(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm()
    resources = SlurmImagerResources(ntasks=7, ntasks_per_node=7)

    with pytest.raises(SlurmLifecycleError, match="expected 6"):
        run_setonix_imager(
            workdir,
            config,
            resources=resources,
            environment=environment,
            run_command=fake,
        )

    assert fake.calls == []


def test_nested_imager_rejects_extra_cpus_per_task(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm()
    resources = SlurmImagerResources(cpus_per_task=2)

    with pytest.raises(SlurmLifecycleError, match="one CPU per MPI task"):
        run_setonix_imager(
            workdir,
            config,
            resources=resources,
            environment=environment,
            run_command=fake,
        )

    assert fake.calls == []


@pytest.mark.parametrize(
    ("channels", "channels_per_worker", "expected_tasks"),
    [
        (250, 50, 6),
        (251, 50, 7),
    ],
)
def test_nested_imager_derives_workers_plus_master_from_channels(
    tmp_path, channels, channels_per_worker, expected_tasks
):
    _root, workdir, config, environment = _runtime(tmp_path)
    config.write_text(
        f"Cimager.Channels = [{channels},0]\n"
        f"Cimager.nchanpercore = {channels_per_worker}\n",
        encoding="utf-8",
    )
    fake = FakeSlurm()
    resources = SlurmImagerResources(
        ntasks=expected_tasks,
        ntasks_per_node=expected_tasks,
    )

    run_setonix_imager(
        workdir,
        config,
        resources=resources,
        environment=environment,
        run_command=fake,
    )

    sbatch = next(call for call in fake.calls if call[0] == "sbatch")
    assert f"--ntasks={expected_tasks}" in sbatch


@pytest.mark.parametrize(
    "config_text",
    [
        "Cimager.nchanpercore = 50\n",
        "Cimager.Channels = [250,0]\n",
        (
            "Cimager.Channels = [250,0]\n"
            "Cimager.Channels = [250,0]\n"
            "Cimager.nchanpercore = 50\n"
        ),
    ],
)
def test_nested_imager_rejects_ambiguous_channel_parallelism(tmp_path, config_text):
    _root, workdir, config, environment = _runtime(tmp_path)
    config.write_text(config_text, encoding="utf-8")
    fake = FakeSlurm()

    with pytest.raises(SlurmLifecycleError, match="must define exactly one"):
        run_setonix_imager(
            workdir,
            config,
            environment=environment,
            run_command=fake,
        )

    assert fake.calls == []


def test_nested_imager_rejects_an_invalid_parent_job_identifier(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    environment["SLURM_JOB_ID"] = "90001; scancel 1"
    fake = FakeSlurm()

    with pytest.raises(SlurmLifecycleError, match="numeric outer job identifier"):
        run_setonix_imager(
            workdir,
            config,
            environment=environment,
            run_command=fake,
        )

    assert fake.calls == []


def test_catchable_interruption_cancels_only_the_recorded_child(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm(job_id="12345;setonix", signal_on_queue=True)

    with pytest.raises(SlurmInterrupted) as caught:
        run_setonix_imager(
            workdir,
            config,
            environment=environment,
            poll_interval=0,
            run_command=fake,
        )

    assert caught.value.signal_number == signal.SIGTERM
    assert [call for call in fake.calls if call[0] == "scancel"] == [
        ["scancel", "--clusters=setonix", "--", "12345"]
    ]
    lifecycle = next(workdir.glob(".beampipe-imager.*"))
    assert "queue_state=absent" in (lifecycle / "child-job-cancel-confirmed").read_text()


def test_final_banner_safe_parsable_receipt_is_accepted():
    reference = _parse_job_reference("site module banner\n12345;setonix\n")

    assert reference.parsable == "12345;setonix"


def test_invalid_receipt_recovers_one_exact_held_job_by_unique_name(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm(
        sbatch_stdout="site banner without a receipt\n", recovery_ids=("91",)
    )

    reference = run_setonix_imager(
        workdir,
        config,
        environment=environment,
        poll_interval=0,
        submission_recovery_timeout=0,
        run_command=fake,
    )

    assert reference.parsable == "91"
    recovery_query = next(
        call
        for call in fake.calls
        if call[0] == "squeue" and any(item.startswith("--name=") for item in call)
    )
    recovered_name = next(
        item.split("=", 1)[1] for item in recovery_query if item.startswith("--name=")
    )
    assert recovered_name.startswith("bp-img-session-unsafe-value-beam25-")
    assert [call for call in fake.calls if call[0] == "scontrol"] == [
        ["scontrol", "release", "91"]
    ]
    lifecycle = next(workdir.glob(".beampipe-imager.*"))
    assert (lifecycle / "child-job-recovered").read_text().endswith("job_id=91\n")


def test_ambiguous_receipt_recovery_fails_closed_with_held_name_evidence(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm(sbatch_stdout="12\n13\n", recovery_ids=("12", "13"))

    with pytest.raises(SlurmLifecycleError, match="held child receipt"):
        run_setonix_imager(
            workdir,
            config,
            environment=environment,
            poll_interval=0,
            submission_recovery_timeout=0,
            run_command=fake,
        )

    assert "--hold" in fake.calls[0]
    assert not any(call[0] in {"scontrol", "scancel"} for call in fake.calls)
    lifecycle = next(workdir.glob(".beampipe-imager.*"))
    evidence = (lifecycle / "child-submission-unresolved").read_text()
    assert "submission_state=held" in evidence
    assert "ambiguous held children" in evidence


def test_nested_imager_rejects_workdir_outside_configured_root(tmp_path):
    _root, _workdir, config, environment = _runtime(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    fake = FakeSlurm()

    with pytest.raises(SlurmLifecycleError, match="workdir escapes"):
        run_setonix_imager(
            outside,
            config,
            environment=environment,
            run_command=fake,
        )

    assert fake.calls == []


def test_nested_imager_rejects_cache_as_output_root(tmp_path):
    root, workdir, config, environment = _runtime(tmp_path)
    environment["WALLABY_HIRES_CACHE_ROOT"] = str(root)
    fake = FakeSlurm()

    with pytest.raises(SlurmLifecycleError, match="must differ"):
        run_setonix_imager(
            workdir,
            config,
            environment=environment,
            run_command=fake,
        )

    assert fake.calls == []


def test_post_submission_accounting_failure_requests_exact_cancellation(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm(job_id="777")

    with pytest.raises(SlurmLifecycleError, match="no terminal accounting record"):
        run_setonix_imager(
            workdir,
            config,
            environment=environment,
            accounting_timeout=0,
            run_command=fake,
        )

    assert [call for call in fake.calls if call[0] == "scancel"] == [
        ["scancel", "--", "777"]
    ]


def test_unconfirmed_exact_id_cancellation_is_fatal_and_persisted(tmp_path):
    _root, workdir, config, environment = _runtime(tmp_path)
    fake = FakeSlurm(
        job_id="888",
        scancel_returncode=1,
        cancel_stays_queued=True,
    )

    with pytest.raises(SlurmLifecycleError, match="unable to confirm cancellation"):
        run_setonix_imager(
            workdir,
            config,
            environment=environment,
            accounting_timeout=0,
            cancellation_timeout=0,
            run_command=fake,
        )

    assert [call for call in fake.calls if call[0] == "scancel"] == [
        ["scancel", "--", "888"]
    ]
    lifecycle = next(workdir.glob(".beampipe-imager.*"))
    failure = (lifecycle / "child-job-cancel-failure").read_text()
    assert "job_id=888" in failure
    assert "scancel_returncode=1" in failure
    assert not (lifecycle / "child-job-cancel-confirmed").exists()
