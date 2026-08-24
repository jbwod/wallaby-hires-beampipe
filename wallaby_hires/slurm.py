"""Lifecycle-safe Slurm submission for the Setonix imager graph node."""

from __future__ import annotations

import contextlib
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from .funcs import resolve_staging_root


class SlurmLifecycleError(RuntimeError):
    """Raised when a nested Slurm imager job cannot be managed safely."""


class SlurmInterrupted(SlurmLifecycleError):
    """Raised after a catchable signal causes the exact child job to be cancelled."""

    def __init__(self, signal_number: int):
        self.signal_number = signal_number
        super().__init__(f"interrupted by signal {signal_number}")


@dataclass(frozen=True)
class SlurmImagerResources:
    """Resources requested by each scattered imager copy."""

    partition: str = "work"
    nodes: int = 1
    ntasks: int = 2
    ntasks_per_node: int = 2
    cpus_per_task: int = 1
    memory: str = "12G"
    time_limit: str = "00:20:00"


@dataclass(frozen=True)
class SlurmJobReference:
    """The job identifier returned by ``sbatch --parsable``."""

    job_id: str
    cluster: str | None = None

    @property
    def parsable(self) -> str:
        return f"{self.job_id};{self.cluster}" if self.cluster else self.job_id


@dataclass
class _SignalState:
    number: int | None = None

    def receive(self, signal_number: int, _frame: object) -> None:
        if self.number is None:
            self.number = signal_number


_ACCOUNT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_RESOURCE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:+-]{0,63}\Z")
_TIME_RE = re.compile(r"(?:\d+-)?\d{1,2}:\d{2}:\d{2}\Z")
_SBATCH_RESULT_RE = re.compile(r"(?P<job_id>\d+)(?:;(?P<cluster>[A-Za-z0-9_.-]+))?\Z")
_JOB_ID_RE = re.compile(r"\d+\Z")
_TERMINAL_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}

_RunCommand = Callable[..., subprocess.CompletedProcess[str]]


def _run_command(args: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(args), **kwargs)  # type: ignore[call-overload]


def _require_positive(name: str, value: int) -> None:
    if value < 1:
        raise ValueError(f"{name} must be positive")


def _validate_resources(resources: SlurmImagerResources) -> None:
    for name, value in (
        ("nodes", resources.nodes),
        ("ntasks", resources.ntasks),
        ("ntasks_per_node", resources.ntasks_per_node),
        ("cpus_per_task", resources.cpus_per_task),
    ):
        _require_positive(name, value)
    if resources.ntasks_per_node > resources.ntasks:
        raise ValueError("ntasks_per_node cannot exceed ntasks")
    if not _RESOURCE_RE.fullmatch(resources.partition):
        raise ValueError("partition contains unsupported characters")
    if not _RESOURCE_RE.fullmatch(resources.memory):
        raise ValueError("memory contains unsupported characters")
    if not _TIME_RE.fullmatch(resources.time_limit):
        raise ValueError("time_limit must use [[days-]hours:]minutes:seconds")


def _safe_tag(value: str, *, maximum: int) -> str:
    tag = re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-")
    return (tag or "unknown")[:maximum]


def _parse_job_reference(stdout: str) -> SlurmJobReference:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    receipts = [
        (index, match)
        for index, line in enumerate(lines)
        if (match := _SBATCH_RESULT_RE.fullmatch(line)) is not None
    ]
    if len(receipts) != 1 or receipts[0][0] != len(lines) - 1:
        raise SlurmLifecycleError(
            "sbatch did not return exactly one final numeric job identifier "
            "in parsable mode"
        )
    match = receipts[0][1]
    return SlurmJobReference(match.group("job_id"), match.group("cluster"))


def _cluster_args(reference: SlurmJobReference) -> list[str]:
    return [f"--clusters={reference.cluster}"] if reference.cluster else []


def _write_evidence(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _single_line_detail(value: str, *, fallback: str) -> str:
    normalized = " ".join(value.split())
    return (normalized or fallback)[:1000]


def _copy_file_secure(source: Path, destination: Path) -> None:
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with (
            source.open("rb") as input_stream,
            os.fdopen(descriptor, "wb") as output_stream,
        ):
            descriptor = -1
            shutil.copyfileobj(input_stream, output_stream)
            output_stream.flush()
            os.fsync(output_stream.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _batch_script(
    config_in_container: str,
    sif: Path,
    staging_root: Path,
    resources: SlurmImagerResources,
    parent_job_id: str | None,
) -> str:
    quoted_config = shlex.quote(config_in_container)
    quoted_sif = shlex.quote(str(sif))
    quoted_root = shlex.quote(str(staging_root))
    quoted_parent_job_id = shlex.quote(parent_job_id or "")
    return f"""#!/bin/bash --login
set -o pipefail

module use /group/askap/modulefiles
module load singularity/4.1.0-askap

export OMP_NUM_THREADS=1
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)

unset SLURM_CPU_BIND SLURM_CPU_BIND_LIST SLURM_CPU_BIND_TYPE SLURM_CPU_BIND_VERBOSE
unset SLURM_MEM_PER_CPU SLURM_MEM_PER_GPU SLURM_MEM_PER_NODE

CONFIG={quoted_config}
ASKAPSOFT_SIF={quoted_sif}
STAGING_ROOT={quoted_root}
PARENT_JOB_ID={quoted_parent_job_id}

echo "IMAGER START $(date)"
echo "HOST=$(hostname)"
echo "PWD=$(pwd)"
echo "CONFIG=${{CONFIG}}"
ls -lh "${{CONFIG}}"

srun -N {resources.nodes} -n {resources.ntasks} -c {resources.cpus_per_task} -m block:block:block \\
  singularity exec \\
    --bind "$PWD:/askapbuffer,${{STAGING_ROOT}}:${{STAGING_ROOT}}" \\
    --pwd /askapbuffer \\
    "${{ASKAPSOFT_SIF}}" \\
    imager -c "${{CONFIG}}" &

imager_pid=$!
parent_lost=0
while kill -0 "${{imager_pid}}" 2>/dev/null; do
  if [ -n "${{PARENT_JOB_ID}}" ]; then
    if parent_rows=$(squeue --noheader --jobs="${{PARENT_JOB_ID}}" --format='%A' 2>/dev/null); then
      if ! printf '%s\n' "${{parent_rows}}" | grep -Fxq -- "${{PARENT_JOB_ID}}"; then
        echo "OUTER JOB ${{PARENT_JOB_ID}} DISAPPEARED; TERMINATING CHILD" >&2
        parent_lost=1
        kill -TERM "${{imager_pid}}" 2>/dev/null || true
        break
      fi
    else
      echo "Unable to query outer job ${{PARENT_JOB_ID}}; retaining child and retrying" >&2
    fi
  fi
  sleep 10
done

wait "${{imager_pid}}"
rc=$?
if [ "${{parent_lost}}" -eq 1 ]; then
  rc=143
fi
echo "IMAGER END $(date), rc=${{rc}}"
ls -lh image* weights* *.fits 2>/dev/null || true
exit "${{rc}}"
"""


@contextlib.contextmanager
def _catch_signals(state: _SignalState) -> Iterator[None]:
    handled = (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
    previous = {item: signal.getsignal(item) for item in handled}
    try:
        for item in handled:
            signal.signal(item, state.receive)
        yield
    finally:
        for item, handler in previous.items():
            signal.signal(item, handler)


def _cancel_job(
    reference: SlurmJobReference,
    *,
    lifecycle_dir: Path,
    run_command: _RunCommand,
    poll_interval: float,
    cancellation_timeout: float,
) -> None:
    result = run_command(
        ["scancel", *_cluster_args(reference), "--", reference.job_id],
        check=False,
        capture_output=True,
        text=True,
    )
    scancel_detail = _single_line_detail(
        result.stderr or "", fallback="no scancel diagnostic"
    )
    deadline = time.monotonic() + cancellation_timeout
    queue_detail = "child remained present in squeue"
    while True:
        try:
            queued = _queued(reference, run_command=run_command)
            if not queued:
                _write_evidence(
                    lifecycle_dir / "child-job-cancel-confirmed",
                    (
                        f"job_id={reference.parsable}\n"
                        f"scancel_returncode={result.returncode}\n"
                        "queue_state=absent\n"
                    ),
                )
                print(
                    "BEAMPIPE_CHILD_CANCEL_CONFIRMED="
                    f"{reference.parsable}|absent|scancel_rc={result.returncode}",
                    flush=True,
                )
                return
            queue_detail = "child remained present in squeue"
        except SlurmLifecycleError as error:
            queue_detail = _single_line_detail(
                str(error), fallback="unknown squeue error"
            )
        if time.monotonic() >= deadline:
            break
        time.sleep(max(poll_interval, 0.1))

    _write_evidence(
        lifecycle_dir / "child-job-cancel-failure",
        (
            f"job_id={reference.parsable}\n"
            f"scancel_returncode={result.returncode}\n"
            f"scancel_detail={scancel_detail}\n"
            f"confirmation_error={queue_detail}\n"
        ),
    )
    print(
        "BEAMPIPE_CHILD_CANCEL_FAILED="
        f"{reference.parsable}|scancel_rc={result.returncode}|{queue_detail}",
        file=sys.stderr,
        flush=True,
    )
    raise SlurmLifecycleError(
        "unable to confirm cancellation of child job "
        f"{reference.parsable}; exact-ID failure evidence is in "
        f"{lifecycle_dir / 'child-job-cancel-failure'}"
    )


def _held_job_candidates(
    job_name: str,
    *,
    run_command: _RunCommand,
) -> list[SlurmJobReference]:
    result = run_command(
        [
            "squeue",
            "--noheader",
            f"--name={job_name}",
            "--states=PENDING",
            "--format=%A|%j|%t|%r",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = _single_line_detail(result.stderr or "", fallback="unknown squeue error")
        raise SlurmLifecycleError(
            f"cannot recover held child by unique name {job_name}: {detail}"
        )

    candidates: set[str] = set()
    for line in result.stdout.splitlines():
        value = line.strip()
        if not value:
            continue
        parts = value.split("|", maxsplit=3)
        if (
            len(parts) != 4
            or not parts[0].isdigit()
            or parts[1] != job_name
            or parts[2] != "PD"
            or parts[3] != "JobHeldUser"
        ):
            raise SlurmLifecycleError(
                "squeue returned an unexpected record while recovering the held "
                f"child named {job_name}"
            )
        candidates.add(parts[0])
    return [SlurmJobReference(job_id) for job_id in sorted(candidates)]


def _recover_held_job_reference(
    job_name: str,
    *,
    run_command: _RunCommand,
    poll_interval: float,
    timeout: float,
) -> SlurmJobReference:
    deadline = time.monotonic() + timeout
    while True:
        candidates = _held_job_candidates(job_name, run_command=run_command)
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            identifiers = ",".join(item.parsable for item in candidates)
            raise SlurmLifecycleError(
                f"ambiguous held children named {job_name}: {identifiers}"
            )
        if time.monotonic() >= deadline:
            raise SlurmLifecycleError(
                f"no held child named {job_name} appeared before the recovery timeout"
            )
        time.sleep(max(poll_interval, 0.1))


def _release_job(
    reference: SlurmJobReference,
    *,
    run_command: _RunCommand,
) -> None:
    result = run_command(
        ["scontrol", *_cluster_args(reference), "release", reference.job_id],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = _single_line_detail(
            result.stderr or "", fallback="unknown scontrol error"
        )
        raise SlurmLifecycleError(
            f"cannot release held child job {reference.parsable}: {detail}"
        )


def _queued(
    reference: SlurmJobReference,
    *,
    run_command: _RunCommand,
) -> bool:
    result = run_command(
        [
            "squeue",
            *_cluster_args(reference),
            "--noheader",
            f"--jobs={reference.job_id}",
            "--format=%A",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = _single_line_detail(result.stderr or "", fallback="unknown squeue error")
        raise SlurmLifecycleError(
            f"cannot query child job {reference.parsable}: {detail}"
        )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if any(line != reference.job_id for line in lines):
        raise SlurmLifecycleError(
            f"squeue returned an unexpected record for child job {reference.parsable}"
        )
    return bool(lines)


def _accounting_record(
    reference: SlurmJobReference,
    *,
    run_command: _RunCommand,
) -> tuple[str, str] | None:
    result = run_command(
        [
            "sacct",
            *_cluster_args(reference),
            "--noheader",
            "--parsable2",
            f"--jobs={reference.job_id}",
            "--format=JobIDRaw,State,ExitCode",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = _single_line_detail(result.stderr or "", fallback="unknown sacct error")
        raise SlurmLifecycleError(
            f"cannot read child job accounting for {reference.parsable}: {detail}"
        )
    for line in result.stdout.splitlines():
        job_id, separator, remainder = line.partition("|")
        state, second_separator, exit_code = remainder.partition("|")
        if separator and second_separator and job_id.strip() == reference.job_id:
            normalized_state = state.strip().split(maxsplit=1)[0].rstrip("+")
            return normalized_state, exit_code.strip()
    return None


def _submission_command(
    *,
    account: str,
    resources: SlurmImagerResources,
    workdir: Path,
    job_name: str,
    stdout_path: Path,
    stderr_path: Path,
    script_path: Path,
) -> list[str]:
    return [
        "sbatch",
        "--parsable",
        "--hold",
        f"--account={account}",
        f"--partition={resources.partition}",
        f"--nodes={resources.nodes}",
        f"--ntasks={resources.ntasks}",
        f"--ntasks-per-node={resources.ntasks_per_node}",
        f"--cpus-per-task={resources.cpus_per_task}",
        f"--mem={resources.memory}",
        f"--time={resources.time_limit}",
        f"--job-name={job_name}",
        f"--chdir={workdir}",
        f"--output={stdout_path}",
        f"--error={stderr_path}",
        "--export=NONE",
        str(script_path),
    ]


def run_setonix_imager(
    workdir: Path,
    config: Path,
    *,
    resources: SlurmImagerResources | None = None,
    environment: Mapping[str, str] | None = None,
    poll_interval: float = 2.0,
    accounting_timeout: float = 60.0,
    submission_recovery_timeout: float = 30.0,
    cancellation_timeout: float = 30.0,
    run_command: _RunCommand = _run_command,
) -> SlurmJobReference:
    """Submit, record, wait for, and safely cancel one scattered imager job."""

    resources = resources or SlurmImagerResources()
    _validate_resources(resources)
    for timeout_name, timeout_value in (
        ("poll_interval", poll_interval),
        ("accounting_timeout", accounting_timeout),
        ("submission_recovery_timeout", submission_recovery_timeout),
        ("cancellation_timeout", cancellation_timeout),
    ):
        if timeout_value < 0:
            raise ValueError(f"{timeout_name} cannot be negative")
    environment = environment or os.environ

    account = (environment.get("BEAMPIPE_SLURM_ACCOUNT") or "").strip()
    sif_value = (environment.get("BEAMPIPE_ASKAPSOFT_SIF") or "").strip()
    if not _ACCOUNT_RE.fullmatch(account):
        raise SlurmLifecycleError(
            "BEAMPIPE_SLURM_ACCOUNT is required and must be a Slurm account name"
        )
    if not sif_value:
        raise SlurmLifecycleError("BEAMPIPE_ASKAPSOFT_SIF is required")
    parent_job_id = (environment.get("SLURM_JOB_ID") or "").strip()
    if parent_job_id and not _JOB_ID_RE.fullmatch(parent_job_id):
        raise SlurmLifecycleError("SLURM_JOB_ID must be a numeric outer job identifier")

    root_value = (environment.get("WALLABY_HIRES_STAGING_ROOT") or "").strip()
    staging_root = Path(resolve_staging_root(root_value)).resolve(strict=True)
    if not staging_root.is_dir():
        raise SlurmLifecycleError(
            f"WALLABY_HIRES_STAGING_ROOT is not a directory: {staging_root}"
        )

    workdir = workdir.expanduser().resolve(strict=True)
    if not workdir.is_dir():
        raise SlurmLifecycleError(f"imager workdir is not a directory: {workdir}")
    if workdir != staging_root and staging_root not in workdir.parents:
        raise SlurmLifecycleError("imager workdir escapes WALLABY_HIRES_STAGING_ROOT")
    config = config.expanduser()
    if not config.is_absolute():
        config = workdir / config
    config = config.resolve(strict=True)
    if not config.is_file():
        raise SlurmLifecycleError(f"imager config is not a regular file: {config}")
    sif = Path(sif_value).expanduser()
    if not sif.is_absolute():
        raise SlurmLifecycleError("BEAMPIPE_ASKAPSOFT_SIF must be an absolute path")
    sif = sif.resolve(strict=True)
    if not sif.is_file() or not os.access(sif, os.R_OK):
        raise SlurmLifecycleError(f"ASKAPsoft SIF is not a readable file: {sif}")

    old_umask = os.umask(0o077)
    try:
        lifecycle_dir = Path(
            tempfile.mkdtemp(prefix=".beampipe-imager.", dir=str(workdir))
        )
    finally:
        os.umask(old_umask)
    os.chmod(lifecycle_dir, 0o700)

    random_tag = lifecycle_dir.name.rsplit(".", maxsplit=1)[-1]
    session_value = (
        environment.get("DLG_SESSION_ID")
        or environment.get("BEAMPIPE_EXECUTION_ID")
        or environment.get("SLURM_JOB_ID")
        or "standalone"
    )
    session_tag = _safe_tag(session_value, maximum=36)
    beam_tag = _safe_tag(workdir.name, maximum=24)
    job_name = f"bp-img-{session_tag}-{beam_tag}-{random_tag}"[:100]

    config_copy = lifecycle_dir / "imager.in"
    _copy_file_secure(config, config_copy)
    config_in_container = f"/askapbuffer/{lifecycle_dir.name}/{config_copy.name}"
    script_path = lifecycle_dir / "imager-job.sh"
    _write_evidence(
        script_path,
        _batch_script(
            config_in_container,
            sif,
            staging_root,
            resources,
            parent_job_id or None,
        ),
    )
    os.chmod(script_path, 0o700)
    _write_evidence(lifecycle_dir / "child-job-name", f"{job_name}\n")

    state = _SignalState()
    reference: SlurmJobReference | None = None
    terminal = False
    try:
        with _catch_signals(state):
            submission = run_command(
                _submission_command(
                    account=account,
                    resources=resources,
                    workdir=workdir,
                    job_name=job_name,
                    stdout_path=lifecycle_dir / "imager-%j.out",
                    stderr_path=lifecycle_dir / "imager-%j.err",
                    script_path=script_path,
                ),
                check=False,
                capture_output=True,
                text=True,
            )
            if submission.returncode != 0:
                detail = _single_line_detail(
                    submission.stderr or "", fallback="unknown sbatch error"
                )
                raise SlurmLifecycleError(f"imager sbatch submission failed: {detail}")
            try:
                reference = _parse_job_reference(submission.stdout)
            except SlurmLifecycleError as parse_error:
                try:
                    reference = _recover_held_job_reference(
                        job_name,
                        run_command=run_command,
                        poll_interval=poll_interval,
                        timeout=submission_recovery_timeout,
                    )
                except SlurmLifecycleError as recovery_error:
                    unresolved = (
                        f"job_name={job_name}\n"
                        "submission_state=held\n"
                        "receipt_error="
                        f"{_single_line_detail(str(parse_error), fallback='invalid receipt')}\n"
                        "recovery_error="
                        f"{_single_line_detail(str(recovery_error), fallback='unknown recovery error')}\n"
                    )
                    _write_evidence(
                        lifecycle_dir / "child-submission-unresolved",
                        unresolved,
                    )
                    raise SlurmLifecycleError(
                        "sbatch succeeded but the held child receipt could not be "
                        f"resolved safely; inspect exact job name {job_name} using "
                        f"{lifecycle_dir / 'child-submission-unresolved'}"
                    ) from recovery_error
                _write_evidence(
                    lifecycle_dir / "child-job-recovered",
                    f"job_name={job_name}\njob_id={reference.parsable}\n",
                )
            _write_evidence(lifecycle_dir / "child-job-id", f"{reference.parsable}\n")
            print(f"BEAMPIPE_CHILD_JOB_ID={reference.parsable}", flush=True)
            print(f"BEAMPIPE_CHILD_JOB_DIR={lifecycle_dir}", flush=True)
            if state.number is not None:
                raise SlurmInterrupted(state.number)
            _release_job(reference, run_command=run_command)
            _write_evidence(
                lifecycle_dir / "child-job-released", f"{reference.parsable}\n"
            )

            while _queued(reference, run_command=run_command):
                if state.number is not None:
                    raise SlurmInterrupted(state.number)
                time.sleep(poll_interval)

            deadline = time.monotonic() + accounting_timeout
            record: tuple[str, str] | None = None
            while record is None and time.monotonic() < deadline:
                if state.number is not None:
                    raise SlurmInterrupted(state.number)
                record = _accounting_record(reference, run_command=run_command)
                if record is None:
                    time.sleep(poll_interval)
            if record is None:
                raise SlurmLifecycleError(
                    f"no terminal accounting record for child job {reference.parsable}"
                )

            job_state, exit_code = record
            if job_state not in _TERMINAL_STATES:
                raise SlurmLifecycleError(
                    f"child job {reference.parsable} has non-terminal state {job_state}"
                )
            terminal = True
            _write_evidence(
                lifecycle_dir / "child-job-final-state",
                f"{job_state}|{exit_code}\n",
            )
            if job_state != "COMPLETED" or exit_code != "0:0":
                raise SlurmLifecycleError(
                    f"child job {reference.parsable} ended {job_state} ({exit_code})"
                )
            print(
                f"BEAMPIPE_CHILD_JOB_FINAL={reference.parsable}|{job_state}|{exit_code}",
                flush=True,
            )
            return reference
    finally:
        if reference is not None and not terminal:
            _cancel_job(
                reference,
                lifecycle_dir=lifecycle_dir,
                run_command=run_command,
                poll_interval=poll_interval,
                cancellation_timeout=cancellation_timeout,
            )
