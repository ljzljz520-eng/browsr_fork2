"""
Atomic, cancellable, resumable download jobs.

Design notes
------------
* The destination name is reserved *before* any byte is transferred:
  a hidden temp file (``.name.<job-id>.part``) and a JSON sidecar are
  created with ``O_EXCL`` inside a cross-process directory lock. Other
  browsr instances (or any process) cannot reserve the same name.
* All bytes land in the temp file. Completion is committed with
  ``flush() + fsync() + os.replace()`` (temp lives in the same
  directory as the destination, hence on the same volume) followed by
  a best-effort directory fsync. A partially written temp file can
  never be mistaken for the completed file.
* On failure / cancellation the temp file is kept as an identifiable
  ``*.part`` plus a ``*.part.json`` sidecar (unless ``keep_partial``
  is ``False``); it is never renamed to the final filename.
* Cancellation is cooperative. A Python thread blocked in a socket
  read cannot be safely killed. The cancel event is honoured at chunk
  boundaries and the source handle is closed to make the blocked read
  fail. Chunks are bounded so worst-case cancel latency is bounded.
* fsspec protocols disagree on seek / length / resume support, so
  resume is *verified at runtime* (``seek`` -> ``tell`` -> first chunk
  length must not exceed the bytes remaining). Any failure — a server
  that ignores ``Range``, a protocol that cannot seek — falls back to
  a fresh download from byte 0. Atomic completion therefore never
  depends on resume capability.
"""

from __future__ import annotations

import contextlib
import datetime
import os
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO, cast

import orjson

#: Bytes per kibibyte (used by :func:`format_bytes`).
BYTES_PER_KIB = 1024

#: Suffix of the in-progress file — always recognizable as incomplete.
PART_SUFFIX = ".part"
#: Suffix of the JSON metadata describing an in-progress download.
SIDECAR_SUFFIX = ".part.json"
#: Per-directory lock file serializing cross-process name reservations.
LOCK_FILENAME = ".browsr-downloads.lock"
#: Bounded chunk size keeps worst-case cancellation latency small.
DEFAULT_CHUNK_SIZE = 1 << 20  # 1 MiB
#: Minimum interval between sidecar metadata rewrites.
SIDECAR_SYNC_INTERVAL = 1.0  # seconds
DEFAULT_DOWNLOAD_DIRNAME = "Downloads"


class JobStatus(str, Enum):
    """
    Lifecycle status of a download job.
    """

    PENDING = "pending"
    RUNNING = "running"
    CANCELING = "canceling"
    CANCELED = "canceled"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        """
        Whether the job has reached a terminal state.
        """
        return self in {
            JobStatus.CANCELED,
            JobStatus.COMPLETED,
            JobStatus.FAILED,
        }


@contextlib.contextmanager
def cross_process_file_lock(lock_path: Path) -> Iterator[None]:
    """
    Exclusive cross-process lock.

    Backed by ``fcntl.flock`` on POSIX and ``msvcrt.locking`` on
    Windows. The lock file itself is never deleted, which avoids the
    classic unlink-then-recreate race.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        if sys.platform == "win32":
            import msvcrt  # noqa: PLC0415

            os.lseek(fd, 0, os.SEEK_SET)
            # LK_LOCK retries internally for ~10 seconds before raising.
            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl  # noqa: PLC0415

            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@dataclass(frozen=True)
class ReservedTarget:
    """
    The three paths involved in one download reservation.
    """

    destination: Path
    temp: Path
    sidecar: Path


@dataclass(frozen=True)
class ProgressSnapshot:
    """
    Immutable point-in-time view of a download job.
    """

    job_id: str
    status: JobStatus
    bytes_transferred: int
    total_size: int | None
    speed: float
    error: str | None = None
    started_at: str | None = None
    source: str | None = None
    destination: str | None = None

    @property
    def fraction_complete(self) -> float | None:
        """
        Fraction of the download completed (``None`` if length unknown).
        """
        if self.total_size:
            return min(self.bytes_transferred / self.total_size, 1.0)
        return None


def format_bytes(num: float) -> str:
    """
    Human-readable byte count.
    """
    value = float(num)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < BYTES_PER_KIB or unit == "TiB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= BYTES_PER_KIB
    return f"{value:.1f} PiB"


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_sidecar(path: Path, payload: dict[str, Any]) -> None:
    """
    Atomically rewrite a sidecar metadata file.
    """
    data = orjson.dumps(payload, option=orjson.OPT_INDENT_2)
    tmp = path.with_name(f"{path.name}.tmp")
    fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)


def read_sidecar(path: Path) -> dict[str, Any] | None:
    """
    Read a sidecar metadata file, returning ``None`` if missing/corrupt.
    """
    try:
        payload = orjson.loads(path.read_bytes())
    except (FileNotFoundError, orjson.JSONDecodeError):
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _part_exists_for(directory: Path, final_name: str) -> bool:
    """
    Whether a ``*.part`` temp file reserves ``final_name``.

    Temp files are named ``.<final_name>.<job-id>.part``. Scanning by
    prefix/suffix (instead of :meth:`Path.glob`) is robust to glob
    metacharacters (``[``, ``*``, ``?``) inside filenames.
    """
    prefix = f".{final_name}."
    try:
        names = os.listdir(directory)
    except FileNotFoundError:
        return False
    return any(
        name.startswith(prefix) and name.endswith(PART_SUFFIX) for name in names
    )


def reserve_target(directory: Path, file_name: str, job_id: str) -> ReservedTarget:
    """
    Atomically reserve a destination name, temp file and sidecar.

    Must be safe against concurrent invocations from other threads and
    other processes. The final name is chosen inside a cross-process
    lock and counts both existing files and temp-file reservations.
    """
    directory.mkdir(parents=True, exist_ok=True)
    lock_path = directory / LOCK_FILENAME
    with cross_process_file_lock(lock_path):
        stem = Path(file_name).stem
        suffix = Path(file_name).suffix
        candidate = file_name
        counter = 1
        while (directory / candidate).exists() or _part_exists_for(
            directory, candidate
        ):
            candidate = f"{stem} ({counter}){suffix}"
            counter += 1

        short_id = job_id.replace("-", "")[:12]
        i = 0
        while True:
            suffix_part = f".{i}" if i else ""
            temp_path = (
                directory / f".{candidate}.{short_id}{suffix_part}{PART_SUFFIX}"
            )
            try:
                fd = os.open(
                    str(temp_path),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
            except FileExistsError:
                i += 1
                continue
            os.close(fd)
            break
        sidecar_path = directory / f"{temp_path.name}.json"
    return ReservedTarget(
        destination=directory / candidate,
        temp=temp_path,
        sidecar=sidecar_path,
    )


def fsync_directory(directory: Path) -> None:
    """
    Best-effort fsync of a directory so a rename survives power loss.

    No-op on Windows, where directory fsync is unavailable.
    """
    if sys.platform == "win32":
        return
    fd: int | None = None
    try:
        fd = os.open(str(directory), os.O_RDONLY)
        os.fsync(fd)
    except OSError:
        # Some network/special filesystems reject fsync; durability of
        # the rename is best-effort, data itself was already synced.
        pass
    finally:
        if fd is not None:
            os.close(fd)


class _RestartFromZeroError(Exception):
    """
    Internal: a resume attempt proved unsupported, restart from byte 0.
    """


class DownloadJob:
    """
    A single download job with a unique ``job_id``.

    The public attributes are simple values and may be read from any
    thread; mutations happen on the worker thread (and on the calling
    thread via :meth:`cancel`).
    """

    def __init__(
        self,
        job_id: str,
        source: Any,
        target: ReservedTarget,
        total_size: int | None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        keep_partial: bool = True,
    ) -> None:
        self.job_id = job_id
        self.source = source
        self.target = target
        self.total_size = total_size
        self.chunk_size = chunk_size
        self.keep_partial = keep_partial

        self.status = JobStatus.PENDING
        self.bytes_transferred = 0
        self.error: str | None = None
        self.started_at: datetime.datetime | None = None
        self.cancel_event = threading.Event()

        self._lock = threading.Lock()
        self._src_handle: Any = None
        self._thread: threading.Thread | None = None
        self._speed = 0.0
        self._snap_ts = time.monotonic()
        self._snap_bytes = 0
        self._last_sidecar_ts = 0.0
        self.terminal_callback: Callable[[DownloadJob], None] | None = None

    # ------------------------------------------------------------------
    # Lifecycle (called by the manager / UI thread)
    # ------------------------------------------------------------------
    def start_thread(self) -> None:
        """
        Publish initial metadata and start the worker thread.
        """
        self.started_at = datetime.datetime.now(datetime.timezone.utc)
        with self._lock:
            self.status = JobStatus.RUNNING
        self.bytes_transferred = self._existing_bytes()
        self._snap_ts = time.monotonic()
        self._snap_bytes = self.bytes_transferred
        self._sync_sidecar(force=True)
        thread = threading.Thread(
            target=self._run_safely,
            name=f"download-{self.job_id[:8]}",
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def cancel(self) -> None:
        """
        Request cooperative cancellation.

        The status becomes :data:`JobStatus.CANCELING` immediately, the
        source handle is closed to unblock a socket read, and the worker
        thread reaches :data:`JobStatus.CANCELED` at the next chunk
        boundary.
        """
        with self._lock:
            if self.status.is_terminal or self.status == JobStatus.CANCELING:
                return
            self.status = JobStatus.CANCELING
        self.cancel_event.set()
        self._close_source()

    def join(self, timeout: float | None = None) -> None:
        """
        Wait for the worker thread to finish.
        """
        if self._thread is not None:
            self._thread.join(timeout)

    def snapshot(self) -> ProgressSnapshot:
        """
        Capture a point-in-time progress snapshot (speed EMA included).
        """
        now = time.monotonic()
        elapsed = now - self._snap_ts
        if elapsed > 0:
            instant = (self.bytes_transferred - self._snap_bytes) / elapsed
            if self._speed == 0.0:
                self._speed = instant
            else:
                self._speed = 0.5 * instant + 0.5 * self._speed
            self._snap_ts = now
            self._snap_bytes = self.bytes_transferred
        with self._lock:
            status = self.status
        return ProgressSnapshot(
            job_id=self.job_id,
            status=status,
            bytes_transferred=self.bytes_transferred,
            total_size=self.total_size,
            speed=self._speed,
            error=self.error,
            started_at=(
                self.started_at.isoformat() if self.started_at is not None else None
            ),
            source=str(self.source),
            destination=str(self.target.destination),
        )

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------
    def _run_safely(self) -> None:
        try:
            self._run()
        except Exception as exc:  # pragma: no cover - defensive last resort
            self._fail(f"{type(exc).__name__}: {exc}")

    def _run(self) -> None:
        resuming = self.bytes_transferred > 0
        try:
            self._stream(resuming=resuming)
        except _RestartFromZeroError:
            if self.cancel_event.is_set():
                self._terminate_canceled()
                return
            self._truncate_temp()
            self.bytes_transferred = 0
            self._speed = 0.0
            self._stream(resuming=False)

    def _stream(self, resuming: bool) -> None:
        """
        One streaming attempt. May raise :class:`_RestartFromZeroError`
        when a resume attempt proves unsupported.
        """
        if self.cancel_event.is_set():
            self._terminate_canceled()
            return
        offset = self.bytes_transferred
        if resuming and offset > 0:
            # Discard anything past the last persisted byte mark.
            self._truncate_temp(to=offset)

        src = self.source.open("rb")
        self._register_source(src)
        try:
            if resuming and offset > 0:
                try:
                    src.seek(offset)
                    if int(src.tell()) != offset:
                        raise _RestartFromZeroError
                except _RestartFromZeroError:
                    raise
                except Exception as exc:
                    raise _RestartFromZeroError from exc

            mode = "ab" if resuming and offset > 0 else "wb"
            with open(self.target.temp, mode) as dest:
                eof = self._copy_loop(
                    src=src, dest=cast(BinaryIO, dest), resuming=resuming
                )
                if not eof:
                    return
                if (
                    self.total_size is not None
                    and self.bytes_transferred != self.total_size
                ):
                    self._fail(
                        f"Downloaded {self.bytes_transferred} bytes, "
                        f"expected {self.total_size}"
                    )
                    return
                dest.flush()
                os.fsync(dest.fileno())
        finally:
            self._close_source()
        self._commit()

    def _copy_loop(
        self, src: Any, dest: BinaryIO, resuming: bool
    ) -> bool:
        """
        Stream chunks from ``src`` into ``dest``.

        Returns ``True`` at clean EOF, ``False`` when canceled. Raises
        :class:`_RestartFromZeroError` only when the *first* fetch of
        a resume attempt fails or is clearly served from byte 0.
        """
        remaining = (
            self.total_size - self.bytes_transferred
            if self.total_size is not None
            else None
        )
        first_chunk = resuming
        while True:
            if self.cancel_event.is_set():
                self._flush_partial(dest)
                self._terminate_canceled()
                return False
            try:
                chunk = src.read(self.chunk_size)
            except Exception as exc:
                if self.cancel_event.is_set():
                    self._flush_partial(dest)
                    self._terminate_canceled()
                    return False
                if first_chunk:
                    raise _RestartFromZeroError from exc
                raise
            if first_chunk:
                # A server that ignores Range would serve the whole file
                # from the start: the first chunk would exceed the bytes
                # that remain from `offset`.
                if chunk and remaining is not None and len(chunk) > remaining:
                    raise _RestartFromZeroError
                first_chunk = False
            if not chunk:
                return True
            dest.write(chunk)
            self.bytes_transferred += len(chunk)
            self._maybe_report()

    # ------------------------------------------------------------------
    # Completion / failure transitions
    # ------------------------------------------------------------------
    def _commit(self) -> None:
        """
        Atomically publish the completed file.

        ``os.replace`` is atomic on POSIX and Windows and, because temp
        and destination share a directory, they share a volume.
        """
        try:
            os.replace(self.target.temp, self.target.destination)
        except OSError as exc:
            # Data is fully downloaded but the commit failed (AV scan,
            # transient sharing violation) — keep a retryable partial.
            self._fail(f"Failed to commit download: {exc}")
            return
        fsync_directory(self.target.destination.parent)
        with contextlib.suppress(FileNotFoundError, OSError):
            self.target.sidecar.unlink()
        with contextlib.suppress(FileNotFoundError, OSError):
            self.target.sidecar.with_name(f"{self.target.sidecar.name}.tmp").unlink()
        self._set_terminal(JobStatus.COMPLETED, persist_metadata=False)

    def _fail(self, message: str) -> None:
        self.error = message
        self._set_terminal(JobStatus.FAILED)

    def _terminate_canceled(self) -> None:
        self._set_terminal(JobStatus.CANCELED)

    def _set_terminal(
        self, status: JobStatus, *, persist_metadata: bool = True
    ) -> None:
        with self._lock:
            if self.status.is_terminal:
                return
            self.status = status
        self._close_source()
        if persist_metadata:
            self._sync_sidecar(force=True)
            if not self.keep_partial:
                self._cleanup_partial()
        if self.terminal_callback is not None:
            try:
                self.terminal_callback(self)
            except Exception:  # noqa: S110
                pass  # pragma: no cover - UI must never break the worker

    def _cleanup_partial(self) -> None:
        with contextlib.suppress(FileNotFoundError, OSError):
            self.target.temp.unlink()
        with contextlib.suppress(FileNotFoundError, OSError):
            self.target.sidecar.unlink()

    # ------------------------------------------------------------------
    # Metadata / handle helpers
    # ------------------------------------------------------------------
    def _sidecar_payload(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "source": str(self.source),
            "destination": str(self.target.destination),
            "temp": str(self.target.temp),
            "bytes": self.bytes_transferred,
            "total": self.total_size,
            "status": self.status.value,
            "error": self.error,
            "started_at": (
                self.started_at.isoformat() if self.started_at is not None else None
            ),
            "updated_at": _now_iso(),
            "chunk_size": self.chunk_size,
        }

    def _sync_sidecar(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_sidecar_ts < SIDECAR_SYNC_INTERVAL:
            return
        self._last_sidecar_ts = now
        with contextlib.suppress(OSError):
            write_sidecar(self.target.sidecar, self._sidecar_payload())

    def _maybe_report(self) -> None:
        now = time.monotonic()
        if now - self._last_sidecar_ts >= SIDECAR_SYNC_INTERVAL:
            self._last_sidecar_ts = now
            self._sync_sidecar()

    def _existing_bytes(self) -> int:
        """
        Resume offset for an existing temp file.

        Prefers the byte count from the sidecar (the last persisted
        progress mark) over the raw temp size, which may include a chunk
        written after the final metadata update.
        """
        try:
            size = self.target.temp.stat().st_size
        except FileNotFoundError:
            return 0
        payload = read_sidecar(self.target.sidecar)
        sidecar_bytes = payload.get("bytes") if payload is not None else None
        if isinstance(sidecar_bytes, int) and 0 <= sidecar_bytes <= size:
            return sidecar_bytes
        return size

    def _truncate_temp(self, to: int = 0) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.truncate(self.target.temp, to)

    @staticmethod
    def _flush_partial(dest: BinaryIO) -> None:
        """
        Flush/fsync the bytes written so far so the kept partial is
        durable and consistent with its metadata.
        """
        with contextlib.suppress(ValueError, OSError):
            dest.flush()
            os.fsync(dest.fileno())

    def _register_source(self, handle: Any) -> None:
        with self._lock:
            self._src_handle = handle

    def _close_source(self) -> None:
        with self._lock:
            handle = self._src_handle
            self._src_handle = None
        if handle is not None:
            with contextlib.suppress(Exception):
                handle.close()


# ----------------------------------------------------------------------
# Manager
# ----------------------------------------------------------------------
TerminalCallback = Callable[[DownloadJob], None]


def _source_name(source: Any) -> str:
    name = getattr(source, "name", None)
    if isinstance(name, str) and name:
        return name
    path = getattr(source, "path", None)
    if path:
        return Path(str(path)).name
    return Path(str(source)).name


def _source_size(source: Any) -> int | None:
    try:
        info = source.fs.info(source.path)
    except Exception:
        return None
    size: Any = None
    if isinstance(info, dict):
        size = info.get("size")
    else:
        size = getattr(info, "st_size", None)
    return size if isinstance(size, int) and size >= 0 else None


class DownloadManager:
    """
    Owns download jobs for one application session.

    Terminal job events are delivered via ``terminal_callback`` (the
    callback runs on the job's worker thread).
    """

    def __init__(self, terminal_callback: TerminalCallback | None = None) -> None:
        self._jobs: dict[str, DownloadJob] = {}
        self._terminal_callback = terminal_callback
        self._lock = threading.Lock()

    @property
    def jobs(self) -> dict[str, DownloadJob]:
        """
        Snapshot of all known jobs keyed by job-id.
        """
        return dict(self._jobs)

    def get_job(self, job_id: str) -> DownloadJob | None:
        """
        Get a job by job-id.
        """
        return self._jobs.get(job_id)

    def start(
        self,
        source: Any,
        download_dir: str | Path | None = None,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        keep_partial: bool = True,
    ) -> str:
        """
        Reserve a target and start a new download job.

        Returns the new job-id.
        """
        job_id = uuid.uuid4().hex
        directory = (
            Path(download_dir).expanduser()
            if download_dir is not None
            else Path.home() / DEFAULT_DOWNLOAD_DIRNAME
        )
        target = reserve_target(
            directory=directory, file_name=_source_name(source), job_id=job_id
        )
        self._stop_active_for(str(target.destination))
        job = DownloadJob(
            job_id=job_id,
            source=source,
            target=target,
            total_size=_source_size(source),
            chunk_size=chunk_size,
            keep_partial=keep_partial,
        )
        job.terminal_callback = self._job_terminated
        with self._lock:
            self._jobs[job_id] = job
        job.start_thread()
        return job_id

    def retry(self, job_id: str) -> str:
        """
        Retry a terminal job. Returns a *new* job-id.

        When the partial files remain the reserved destination name is
        reused and the download resumes if the source supports it; when
        they were removed a fresh name is reserved.
        """
        old = self._require_job(job_id)
        if not old.status.is_terminal:
            msg = f"Job {job_id} is not terminal ({old.status.value})"
            raise RuntimeError(msg)
        new_id = uuid.uuid4().hex
        if old.target.temp.exists():
            target = old.target
        else:
            target = reserve_target(
                directory=old.target.destination.parent,
                file_name=old.target.destination.name,
                job_id=new_id,
            )
        self._stop_active_for(str(target.destination))
        job = DownloadJob(
            job_id=new_id,
            source=old.source,
            target=target,
            total_size=_source_size(old.source),
            chunk_size=old.chunk_size,
            keep_partial=old.keep_partial,
        )
        job.terminal_callback = self._job_terminated
        with self._lock:
            self._jobs[new_id] = job
        job.start_thread()
        return new_id

    def cancel(self, job_id: str) -> None:
        """
        Request cancellation of a job.
        """
        job = self._jobs.get(job_id)
        if job is not None:
            job.cancel()

    def snapshot(self, job_id: str) -> ProgressSnapshot | None:
        """
        Get a progress snapshot for a job.
        """
        job = self._jobs.get(job_id)
        return job.snapshot() if job is not None else None

    def _require_job(self, job_id: str) -> DownloadJob:
        try:
            return self._jobs[job_id]
        except KeyError:
            msg = f"Unknown download job-id: {job_id}"
            raise KeyError(msg) from None

    def _job_terminated(self, job: DownloadJob) -> None:
        if self._terminal_callback is not None:
            try:
                self._terminal_callback(job)
            except Exception:  # noqa: S110
                pass  # pragma: no cover - UI errors must not propagate

    def _stop_active_for(self, destination: str) -> None:
        """
        Guarantee a single writer per destination (defensive; the UI
        only retries terminal jobs).
        """
        for job in list(self._jobs.values()):
            if (
                str(job.target.destination) == destination
                and not job.status.is_terminal
            ):
                job.cancel()
                job.join(30)
