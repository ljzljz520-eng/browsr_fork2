"""
Confirmation Widget

A single pop up that walks through the whole download lifecycle:

1. ``confirm`` — asks the user whether to download.
2. ``downloading`` — shows a progress bar (bytes, speed) and a cancel
   button.
3. ``terminal`` — reports completion, failure or cancellation and
   offers retry / close.

Every message that refers to a download carries its ``job_id``. The pop
up only reacts to messages for the job it is currently bound to, so a
stale (old) job can never close or overwrite a newer overlay.
"""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Any

from rich.markdown import Markdown
from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.events import Key
from textual.message import Message
from textual.timer import Timer
from textual.widgets import Button, ProgressBar, Static

from browsr.downloader import DEFAULT_DOWNLOAD_DIRNAME, JobStatus, format_bytes
from browsr.widgets.base import BaseOverlay, BasePopUp

if TYPE_CHECKING:
    from browsr.downloader import DownloadManager, ProgressSnapshot

#: Pop-up state constants.
STATE_CONFIRM = "confirm"
STATE_DOWNLOADING = "downloading"
STATE_TERMINAL = "terminal"

#: How often the progress view is polled while downloading.
PROGRESS_POLL_INTERVAL = 0.2

#: Label shown in the terminal view for retained partial files.
PART_LABEL = "*.part"


class ConfirmationPopUp(BasePopUp):
    """
    A Pop Up that confirms, tracks and retries a download.
    """

    class ConfirmationWindowDownload(Message):
        """
        The user confirmed the download prompt (no job exists yet).
        """

    class RetryRequested(Message):
        """
        The user requested a retry of a terminal job.
        """

        def __init__(self, job_id: str) -> None:
            self.job_id = job_id
            super().__init__()

    class JobFinished(Message):
        """
        A download job reached a terminal state.
        """

        def __init__(
            self,
            job_id: str,
            status: JobStatus,
            error: str | None = None,
            destination: str | None = None,
        ) -> None:
            self.job_id = job_id
            self.status = status
            self.error = error
            self.destination = destination
            super().__init__()

    class DisplayToggle(Message):
        """
        The confirmation overlay should be closed.

        A ``None`` job-id is an unbound dismiss (always honoured); a
        concrete job-id is only honoured while it remains the bound
        job.
        """

        def __init__(self, job_id: str | None = None) -> None:
            self.job_id = job_id
            super().__init__()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.state = STATE_CONFIRM
        self.current_job_id: str | None = None
        self._terminal_status: JobStatus | None = None
        self._manager: DownloadManager | None = None
        self._poll_interval: Timer | None = None

    def compose(self) -> ComposeResult:
        """
        Compose the Confirmation Pop Up
        """
        self.download_message = Static(Markdown(""))
        yield self.download_message
        self.progress_bar = ProgressBar(
            show_eta=False, id="download-progress"
        )
        yield self.progress_bar
        self.progress_detail = Static("", id="progress-detail")
        yield self.progress_detail
        with Horizontal(id="confirmation-buttons"):
            yield Button("Yes (y)", variant="success", id="confirm-yes")
            yield Button("No (n)", variant="error", id="confirm-no")
            yield Button("Cancel", variant="warning", id="download-cancel")
            yield Button("Retry (r)", variant="success", id="download-retry")
            yield Button("Close", variant="primary", id="download-close")

    def prompt_download(self, file_path: str) -> None:
        """
        Reset to the confirmation prompt for a file.
        """
        prompt_message: str = dedent(
            f"""
            ## File Download

            **Are you sure you want to download that file?**

            **File:** `{file_path}`

            **Save to:** `~/{DEFAULT_DOWNLOAD_DIRNAME}`
            """
        )
        self._stop_polling()
        self.state = STATE_CONFIRM
        self.current_job_id = None
        self._manager = None
        self._terminal_status = None
        self.download_message.update(Markdown(prompt_message))
        self._apply_visibility()
        self.refresh()

    def attach_job(self, job_id: str, manager: DownloadManager) -> None:
        """
        Bind the pop up to a running download job.
        """
        self._stop_polling()
        self.current_job_id = job_id
        self._manager = manager
        self._terminal_status = None
        self.state = STATE_DOWNLOADING
        self.download_message.update(Markdown("## Downloading"))
        self.progress_bar.update(progress=0.0, total=None)
        self.progress_detail.update("Starting...")
        self._apply_visibility()
        self._poll_interval = self.set_interval(
            PROGRESS_POLL_INTERVAL, self._poll_progress
        )

    def action_close(self) -> None:
        """
        Close / cancel depending on the current state.

        While downloading, Escape requests cancellation rather than
        dismissing, so the user stays informed of the terminal state.
        """
        if self.state == STATE_DOWNLOADING:
            self._request_cancel()
            return
        self._dismiss()

    @on(Button.Pressed)
    def handle_download_selection(self, message: Button.Pressed) -> None:
        """
        Handle Button Presses
        """
        button_id = message.button.id
        if button_id == "confirm-yes":
            self.post_message(self.ConfirmationWindowDownload())
        elif button_id in {"confirm-no", "download-close"}:
            self._dismiss()
        elif button_id == "download-cancel":
            self._request_cancel()
        elif button_id == "download-retry" and self.current_job_id is not None:
            self.post_message(self.RetryRequested(self.current_job_id))

    @on(Key)
    def handle_key_press(self, message: Key) -> None:
        """
        Handle Key Presses
        """
        key = message.key.lower()
        if self.state == STATE_CONFIRM:
            if key == "y":
                self.post_message(self.ConfirmationWindowDownload())
            elif key == "n":
                self._dismiss()
        elif (
            self.state == STATE_TERMINAL
            and key == "r"
            and self._terminal_status is not JobStatus.COMPLETED
        ):
            if self.current_job_id is not None:
                self.post_message(self.RetryRequested(self.current_job_id))

    @on(JobFinished)
    def handle_job_finished(self, message: JobFinished) -> None:
        """
        Handle a terminal job event.

        Stale job-ids are ignored: an old job finishing must not alter
        the overlay of a newer job.
        """
        if message.job_id != self.current_job_id:
            return
        snapshot = (
            self._manager.snapshot(message.job_id)
            if self._manager is not None
            else None
        )
        if snapshot is not None:
            self._render_terminal(snapshot)
        else:
            self._stop_polling()
            self.state = STATE_TERMINAL
            self._terminal_status = message.status
            self._apply_visibility()

    # ------------------------------------------------------------------
    # Progress / terminal rendering
    # ------------------------------------------------------------------
    def _poll_progress(self) -> None:
        """
        Refresh the progress view from the bound job.
        """
        if self._manager is None or self.current_job_id is None:
            return
        snapshot = self._manager.snapshot(self.current_job_id)
        if snapshot is None or snapshot.job_id != self.current_job_id:
            return
        if snapshot.status.is_terminal:
            self._render_terminal(snapshot)
            return
        self._update_progress_view(snapshot)

    def _update_progress_view(self, snapshot: ProgressSnapshot) -> None:
        """
        Update the progress bar and detail line.
        """
        total = snapshot.total_size
        self.progress_bar.update(
            progress=float(snapshot.bytes_transferred),
            total=float(total) if total is not None else None,
        )
        detail = (
            f"{format_bytes(snapshot.bytes_transferred)} / "
            f"{format_bytes(total)}"
            if total is not None
            else format_bytes(snapshot.bytes_transferred)
        )
        if snapshot.status is JobStatus.CANCELING:
            detail += " | Canceling..."
        elif snapshot.speed > 0:
            detail += f" | {format_bytes(snapshot.speed)}/s"
        self.progress_detail.update(detail)

    def _render_terminal(self, snapshot: ProgressSnapshot) -> None:
        """
        Transition to the terminal view.
        """
        self._stop_polling()
        self.state = STATE_TERMINAL
        self._terminal_status = snapshot.status
        if snapshot.status is JobStatus.COMPLETED:
            self.download_message.update(
                Markdown(
                    dedent(
                        f"""
                        ## Download Complete

                        **Saved to:** `{snapshot.destination}`
                        """
                    )
                )
            )
            self.progress_bar.update(
                progress=float(snapshot.bytes_transferred),
                total=float(
                    snapshot.total_size
                    if snapshot.total_size is not None
                    else max(snapshot.bytes_transferred, 1)
                ),
            )
            self.progress_detail.update(
                f"{format_bytes(snapshot.bytes_transferred)} complete"
            )
        else:
            title = (
                "Download Canceled"
                if snapshot.status is JobStatus.CANCELED
                else "Download Failed"
            )
            error = (
                f"\n\n**Error:** `{snapshot.error}`"
                if snapshot.error
                else ""
            )
            self.download_message.update(Markdown(f"## {title}{error}"))
            if snapshot.bytes_transferred:
                self.progress_detail.update(
                    f"{format_bytes(snapshot.bytes_transferred)} "
                    f"partial retained ({PART_LABEL})"
                )
            else:
                self.progress_detail.update("")
        self._apply_visibility()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _request_cancel(self) -> None:
        """
        Request cooperative cancellation of the bound job.
        """
        if self._manager is not None and self.current_job_id is not None:
            self._manager.cancel(self.current_job_id)
        self.progress_detail.update("Canceling...")

    def _dismiss(self) -> None:
        """
        Dismiss the pop up and ask the host to close the overlay.
        """
        job_id = self.current_job_id
        self._stop_polling()
        super().action_close()
        self.post_message(self.DisplayToggle(job_id))

    def _stop_polling(self) -> None:
        """
        Stop the progress polling interval.
        """
        if self._poll_interval is not None:
            self._poll_interval.stop()
            self._poll_interval = None

    def _apply_visibility(self) -> None:
        """
        Show only the widgets relevant to the current state.
        """
        in_confirm = self.state == STATE_CONFIRM
        in_download = self.state == STATE_DOWNLOADING
        completed = self._terminal_status is JobStatus.COMPLETED
        terminal = self.state == STATE_TERMINAL
        self.query_one("#confirm-yes", Button).display = in_confirm
        self.query_one("#confirm-no", Button).display = in_confirm
        self.query_one("#download-cancel", Button).display = in_download
        self.query_one("#download-retry", Button).display = (
            terminal and not completed
        )
        self.query_one("#download-close", Button).display = terminal
        self.progress_bar.display = in_download or (terminal and completed)
        self.progress_detail.display = not in_confirm


class ConfirmationWindow(BaseOverlay):
    """
    Window containing the Confirmation Pop Up
    """

    def action_close(self) -> None:
        """
        Close the overlay and notify the host.
        """
        super().action_close()
        self.post_message(ConfirmationPopUp.DisplayToggle())
