"""
Tests for the atomic, cancellable download jobs and the confirmation
pop up lifecycle.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import fsspec
import pytest
from textual.app import App, ComposeResult
from textual.widgets import Button

from browsr.downloader import (
    LOCK_FILENAME,
    DownloadManager,
    JobStatus,
    ProgressSnapshot,
    read_sidecar,
    reserve_target,
)
from browsr.widgets.confirmation import (
    STATE_DOWNLOADING,
    STATE_TERMINAL,
    ConfirmationPopUp,
    ConfirmationWindow,
)


# ----------------------------------------------------------------------
# Test doubles
# ----------------------------------------------------------------------
class ControlledFile:
    """
    File-like object whose reads must be released one at a time.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self._condition = threading.Condition()
        self._permits = 0
        self._blocked_reads = 0
        self._eof_reads = 0
        self._closed = False

    def seek(self, pos: int, whence: int = os.SEEK_SET) -> int:
        with self._condition:
            if whence == os.SEEK_SET:
                new_pos = pos
            elif whence == os.SEEK_CUR:
                new_pos = self._pos + pos
            else:
                new_pos = len(self._data) + pos
            self._pos = max(0, new_pos)
            return self._pos

    def tell(self) -> int:
        with self._condition:
            return self._pos

    def read(self, size: int = -1) -> bytes:
        with self._condition:
            if self._pos >= len(self._data):
                self._eof_reads += 1
                self._condition.notify_all()
                return b""
            self._blocked_reads += 1
            self._condition.notify_all()
            while self._permits == 0:
                self._condition.wait()
            self._permits -= 1
            if self._closed:
                raise ValueError("I/O operation on closed file")
            end = (
                len(self._data)
                if size is None or size < 0
                else min(self._pos + size, len(self._data))
            )
            chunk = self._data[self._pos : end]
            self._pos += len(chunk)
            return chunk

    def release_one(self) -> None:
        with self._condition:
            self._permits += 1
            self._condition.notify_all()

    def wait_until_blocked(self, previous: int) -> None:
        deadline = time.monotonic() + 5
        with self._condition:
            while self._blocked_reads <= previous:
                self._condition.wait(0.1)
                if time.monotonic() > deadline:
                    msg = "worker never blocked on a read"
                    raise AssertionError(msg)

    def wait_until_settled(self, previous: int) -> None:
        """
        Wait until the read following the last released chunk completes:
        it either blocks again or reaches EOF.
        """
        deadline = time.monotonic() + 5
        with self._condition:
            while self._blocked_reads + self._eof_reads <= previous:
                self._condition.wait(0.1)
                if time.monotonic() > deadline:
                    msg = "worker never settled after a release"
                    raise AssertionError(msg)

    @property
    def blocked_reads(self) -> int:
        with self._condition:
            return self._blocked_reads

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._permits += 1
            self._condition.notify_all()


class PlainFile:
    """
    Non-gated handle over a real local file.
    """

    def __init__(self, path: Path) -> None:
        self._handle = path.open("rb")

    def seek(self, pos: int, whence: int = 0) -> int:
        return self._handle.seek(pos, whence)

    def tell(self) -> int:
        return self._handle.tell()

    def read(self, size: int = -1) -> bytes:
        return self._handle.read(size)

    def close(self) -> None:
        self._handle.close()


class SeekUnsupportedFile(PlainFile):
    """
    Handle whose seek always fails (e.g. a protocol without resume).
    """

    def seek(self, pos: int, whence: int = 0) -> int:
        raise NotImplementedError("seek not supported")


class RangeIgnoringFile:
    """
    Accepts seeks (and lies on tell) but always serves from byte 0.

    Simulates an HTTP server that ignores ``Range`` requests.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._served = 0
        self._claimed = 0

    def seek(self, pos: int, whence: int = 0) -> int:
        if whence == os.SEEK_SET:
            self._claimed = pos
        return self._claimed

    def tell(self) -> int:
        return self._claimed

    def read(self, size: int = -1) -> bytes:
        end = (
            len(self._data)
            if size is None or size < 0
            else min(self._served + size, len(self._data))
        )
        chunk = self._data[self._served : end]
        self._served = end
        return chunk

    def close(self) -> None:
        pass


class FailAfterFirstFile:
    """
    Serves one chunk then fails every subsequent read.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._chunks = 0

    def read(self, size: int = -1) -> bytes:
        if self._chunks == 0:
            self._chunks += 1
            end = (
                len(self._data)
                if size is None or size < 0
                else min(size, len(self._data))
            )
            return self._data[:end]
        raise OSError("boom")

    def close(self) -> None:
        pass


class FakeSource:
    """
    Minimal fsspec-like source.
    """

    def __init__(
        self, data_path: Path, handle_factory: Callable[[], Any]
    ) -> None:
        self.name = data_path.name
        self.path = str(data_path)
        self.fs = fsspec.filesystem("file")
        self._handle_factory = handle_factory

    def open(self, _mode: str = "rb") -> Any:
        return self._handle_factory()

    def __str__(self) -> str:
        return self.path


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def wait_terminal(
    manager: DownloadManager, job_id: str, timeout: float = 10
) -> ProgressSnapshot:
    """
    Wait for a job to reach a terminal state.
    """
    deadline = time.monotonic() + timeout
    while True:
        snapshot = manager.snapshot(job_id)
        if snapshot is not None and snapshot.status.is_terminal:
            return snapshot
        if time.monotonic() > deadline:
            status = None if snapshot is None else snapshot.status
            msg = f"job {job_id} never terminated (last status {status})"
            raise AssertionError(msg)
        time.sleep(0.01)


def release_chunks(handle: ControlledFile, count: int, seen: int) -> int:
    """
    Release exactly ``count`` blocked reads, waiting for every released
    chunk to actually be served. Returns the new seen (served) count.
    """
    for i in range(count):
        handle.wait_until_blocked(seen + i)
        handle.release_one()
    handle.wait_until_settled(seen + count)
    return seen + count


def write_data(tmp_path: Path, name: str, data: bytes) -> Path:
    """
    Write a source data file.
    """
    data_path = tmp_path / "source" / name
    data_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.write_bytes(data)
    return data_path


def find_files(directory: Path) -> tuple[list[Path], list[Path]]:
    """
    Return ``(part_files, sidecar_files)`` in a directory.
    """
    files = list(directory.iterdir())
    parts = [f for f in files if f.name.endswith(".part")]
    sidecars = [f for f in files if f.name.endswith(".part.json")]
    return parts, sidecars


# ----------------------------------------------------------------------
# Core job behavior
# ----------------------------------------------------------------------
def test_completed_download_commits_atomically(tmp_path: Path) -> None:
    """
    A completed download lands at the final path; no part files remain.
    """
    data = b"hello world " * 10
    data_path = write_data(tmp_path, "data.txt", data)
    download_dir = tmp_path / "downloads"
    source = FakeSource(data_path, lambda: PlainFile(data_path))
    manager = DownloadManager()
    job_id = manager.start(source=source, download_dir=download_dir)
    snapshot = wait_terminal(manager, job_id)

    assert snapshot.status is JobStatus.COMPLETED
    assert snapshot.bytes_transferred == len(data)
    final = download_dir / "data.txt"
    assert final.read_bytes() == data
    names = {path.name for path in download_dir.iterdir()}
    assert names == {"data.txt", LOCK_FILENAME}
    parts, sidecars = find_files(download_dir)
    assert not parts
    assert not sidecars


def test_progress_tracks_bytes_fraction_and_speed(tmp_path: Path) -> None:
    """
    Snapshots report transferred bytes, fraction and a non-zero speed.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    handle = ControlledFile(data)
    source = FakeSource(data_path, lambda: handle)
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=16
    )

    seen = release_chunks(handle, count=4, seen=0)
    snapshot = manager.snapshot(job_id)
    assert snapshot.status is JobStatus.RUNNING
    assert snapshot.bytes_transferred == 64
    assert snapshot.total_size == 256
    assert snapshot.fraction_complete == 0.25

    time.sleep(0.05)
    handle.wait_until_blocked(seen)
    handle.release_one()
    handle.wait_until_blocked(seen + 1)
    snapshot = manager.snapshot(job_id)
    assert snapshot.bytes_transferred == 80
    assert snapshot.speed > 0

    # Release the remaining 11 chunks (16 chunks total cover 256 bytes).
    release_chunks(handle, count=11, seen=seen + 1)
    final_snapshot = wait_terminal(manager, job_id)
    assert final_snapshot.status is JobStatus.COMPLETED
    assert final_snapshot.bytes_transferred == 256


def test_cancel_keeps_identifiable_partial(tmp_path: Path) -> None:
    """
    Cancellation keeps a ``.part`` file plus a matching sidecar, and the
    final destination is never created.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    handle = ControlledFile(data)
    source = FakeSource(data_path, lambda: handle)
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=16
    )
    release_chunks(handle, count=3, seen=0)
    handle.wait_until_blocked(3)

    manager.cancel(job_id)
    snapshot = wait_terminal(manager, job_id)
    assert snapshot.status is JobStatus.CANCELED
    assert snapshot.error is None

    assert not (download_dir / "data.bin").exists()
    parts, sidecars = find_files(download_dir)
    assert len(parts) == 1
    assert len(sidecars) == 1
    assert parts[0].stat().st_size == 48
    payload = read_sidecar(sidecars[0])
    assert payload is not None
    assert payload["job_id"] == job_id
    assert payload["status"] == "canceled"
    assert payload["bytes"] == 48


def test_retry_resumes_transfer(tmp_path: Path) -> None:
    """
    Retrying a canceled job reuses the partial and appends to it.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    first_handle = ControlledFile(data)
    second_handle = ControlledFile(data)
    handles = iter([first_handle, second_handle])
    source = FakeSource(data_path, lambda: next(handles))
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=16
    )
    release_chunks(first_handle, count=3, seen=0)
    first_handle.wait_until_blocked(3)
    manager.cancel(job_id)
    wait_terminal(manager, job_id)

    new_id = manager.retry(job_id)
    assert new_id != job_id
    assert manager.snapshot(new_id).bytes_transferred == 48

    # 13 chunks remain (48 -> 256); second handle starts blocked at read 1.
    release_chunks(second_handle, count=13, seen=0)
    snapshot = wait_terminal(manager, new_id)
    assert snapshot.status is JobStatus.COMPLETED
    assert (download_dir / "data.bin").read_bytes() == data
    parts, sidecars = find_files(download_dir)
    assert not parts
    assert not sidecars


def test_retry_restarts_from_zero_when_seek_unsupported(
    tmp_path: Path,
) -> None:
    """
    When the source cannot seek, resume is abandoned and the download
    restarts from byte 0 — atomic completion does not depend on resume.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    first_handle = ControlledFile(data)

    def factory() -> Any:
        if not switched:
            return first_handle
        return SeekUnsupportedFile(data_path)

    switched = False
    source = FakeSource(data_path, factory)
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=16
    )
    release_chunks(first_handle, count=3, seen=0)
    first_handle.wait_until_blocked(3)
    manager.cancel(job_id)
    wait_terminal(manager, job_id)

    switched = True
    new_id = manager.retry(job_id)
    snapshot = wait_terminal(manager, new_id)
    assert snapshot.status is JobStatus.COMPLETED
    assert snapshot.bytes_transferred == 256
    assert (download_dir / "data.bin").read_bytes() == data
    parts, sidecars = find_files(download_dir)
    assert not parts
    assert not sidecars


def test_retry_restarts_when_server_ignores_range(tmp_path: Path) -> None:
    """
    A server that ignores Range serves from byte 0: the over-long first
    chunk triggers a fresh restart.
    """
    data = bytes(range(200))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    first_handle = ControlledFile(data)

    def factory() -> Any:
        if not switched:
            return first_handle
        return RangeIgnoringFile(data)

    switched = False
    source = FakeSource(data_path, factory)
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=128
    )
    # First chunk = 128 bytes; worker then blocks for the remaining 72.
    first_handle.wait_until_blocked(0)
    first_handle.release_one()
    first_handle.wait_until_blocked(1)
    manager.cancel(job_id)
    wait_terminal(manager, job_id)
    assert first_handle.tell() == 128

    switched = True
    new_id = manager.retry(job_id)
    snapshot = wait_terminal(manager, new_id)
    assert snapshot.status is JobStatus.COMPLETED
    assert (download_dir / "data.bin").read_bytes() == data


def test_failure_keeps_partial_metadata(tmp_path: Path) -> None:
    """
    A mid-transfer failure keeps an identifiable partial with an error
    recorded in the sidecar; it never pretends to be the final file.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    handle = FailAfterFirstFile(data)
    source = FakeSource(data_path, lambda: handle)
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=16
    )
    snapshot = wait_terminal(manager, job_id)
    assert snapshot.status is JobStatus.FAILED
    assert "boom" in (snapshot.error or "")
    assert not (download_dir / "data.bin").exists()

    parts, sidecars = find_files(download_dir)
    assert len(parts) == 1
    assert len(sidecars) == 1
    payload = read_sidecar(sidecars[0])
    assert payload is not None
    assert payload["status"] == "failed"
    assert payload["bytes"] == parts[0].stat().st_size
    assert "boom" in payload["error"]


def test_failure_without_keep_partial_cleans_up(tmp_path: Path) -> None:
    """
    With ``keep_partial=False`` failure removes temp and sidecar.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    source = FakeSource(data_path, lambda: FailAfterFirstFile(data))
    manager = DownloadManager()
    job_id = manager.start(
        source=source,
        download_dir=download_dir,
        chunk_size=16,
        keep_partial=False,
    )
    snapshot = wait_terminal(manager, job_id)
    assert snapshot.status is JobStatus.FAILED
    names = {path.name for path in download_dir.iterdir()}
    assert names == {LOCK_FILENAME}


# ----------------------------------------------------------------------
# Name reservation
# ----------------------------------------------------------------------
def test_reserve_target_handles_duplicates_and_parts(
    tmp_path: Path,
) -> None:
    """
    Existing files and in-progress parts both reserve the name.
    """
    first = reserve_target(tmp_path, "file.txt", "a" * 32)
    assert first.destination.name == "file.txt"
    second = reserve_target(tmp_path, "file.txt", "b" * 32)
    assert second.destination.name == "file (1).txt"

    # Commit the first file while its (now replaced) temp existed:
    first.destination.write_bytes(b"x")
    third = reserve_target(tmp_path, "file.txt", "c" * 32)
    assert third.destination.name == "file (2).txt"


def test_reserve_target_safe_with_glob_metacharacters(
    tmp_path: Path,
) -> None:
    """
    Filenames containing glob metacharacters reserve and dedupe safely.
    """
    first = reserve_target(tmp_path, "report[1].txt", "a" * 32)
    assert first.destination.name == "report[1].txt"
    second = reserve_target(tmp_path, "report[1].txt", "b" * 32)
    assert second.destination.name == "report[1] (1).txt"


def test_retry_without_partial_re_reserves_name(tmp_path: Path) -> None:
    """
    When the partial files are gone, retry reserves the name afresh.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    first_handle = ControlledFile(data)

    def factory() -> Any:
        if not switched:
            return first_handle
        return PlainFile(data_path)

    switched = False
    source = FakeSource(data_path, factory)
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=16
    )
    release_chunks(first_handle, count=3, seen=0)
    first_handle.wait_until_blocked(3)
    manager.cancel(job_id)
    wait_terminal(manager, job_id)

    for file_path in download_dir.iterdir():
        if file_path.name != LOCK_FILENAME:
            file_path.unlink()

    switched = True
    new_id = manager.retry(job_id)
    job = manager.get_job(new_id)
    assert job is not None
    assert job.target.destination.name == "data.bin"
    snapshot = wait_terminal(manager, new_id)
    assert snapshot.status is JobStatus.COMPLETED
    assert (download_dir / "data.bin").read_bytes() == data


# ----------------------------------------------------------------------
# Confirmation pop up lifecycle (stale job guard)
# ----------------------------------------------------------------------
class HostApp(App[None]):
    """
    Minimal host app mounting the confirmation widgets.
    """

    def compose(self) -> ComposeResult:
        self.popup = ConfirmationPopUp()
        yield ConfirmationWindow(self.popup, id="confirmation-container")


async def _wait_popup_state(
    popup: ConfirmationPopUp, state: str, pilot: Any
) -> None:
    deadline = time.monotonic() + 5
    while popup.state != state:
        await pilot.pause(0.05)
        if time.monotonic() > deadline:
            msg = f"popup never reached {state} (in {popup.state})"
            raise AssertionError(msg)


@pytest.mark.asyncio
async def test_stale_job_finished_is_ignored(tmp_path: Path) -> None:
    """
    A JobFinished message for an old job must not alter an overlay bound
    to a newer job; the real job still transitions to terminal.
    """
    data = bytes(range(256))
    data_path = write_data(tmp_path, "data.bin", data)
    download_dir = tmp_path / "downloads"
    handle = ControlledFile(data)
    source = FakeSource(data_path, lambda: handle)
    # No terminal callback: the popup relies on its own polling, so this
    # test exercises the stale-guard inside the JobFinished handler.
    manager = DownloadManager()
    job_id = manager.start(
        source=source, download_dir=download_dir, chunk_size=16
    )

    app = HostApp()
    async with app.run_test() as pilot:
        app.popup.attach_job(job_id, manager)
        await pilot.pause()
        assert app.popup.state == STATE_DOWNLOADING

        app.popup.post_message(
            ConfirmationPopUp.JobFinished(
                job_id="stale-job-id",
                status=JobStatus.FAILED,
                error="old failure",
            )
        )
        await pilot.pause()
        assert app.popup.state == STATE_DOWNLOADING
        assert app.popup.query_one("#download-cancel", Button).display
        assert not app.popup.query_one("#download-retry", Button).display
        assert not app.popup.query_one("#download-close", Button).display

        # Finish the real job (16 chunks of 16 bytes = 256).
        release_chunks(handle, count=16, seen=0)
        await _wait_popup_state(app.popup, STATE_TERMINAL, pilot)
        await pilot.pause()
        assert app.popup._terminal_status is JobStatus.COMPLETED
        assert app.popup.query_one("#download-close", Button).display
        assert not app.popup.query_one("#download-retry", Button).display
