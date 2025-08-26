from __future__ import annotations
import json
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple, Literal, List, Dict

from cachetools.func import ttl_cache
from pydantic import BaseModel, AnyUrl, Field, ValidationError, model_validator


# ---------- time helpers ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------- job status & flap handling ----------
class Status(BaseModel):
    status: Literal["active", "inactive"]
    # Timestamp (UTC) of when this status was observed/changed
    at: datetime = Field(default_factory=now_utc)


_FLAP_WINDOW = timedelta(hours=6)


def _normalize_history(history: List[Status]) -> List[Status]:
    """
    Normalize history:
      - Sort by timestamp ascending.
      - Collapse consecutive duplicate statuses (keep earliest in each run).
      - If we see an 'active' → 'inactive' → 'active' within _FLAP_WINDOW,
        treat it as never inactive: keep the first 'active', drop the
        'inactive' and the returning 'active'.
    """
    if not history:
        return []

    hist = sorted(history, key=lambda s: s.at)
    out: List[Status] = []

    for st in hist:
        if out:
            # 1) Drop consecutive duplicates
            if out[-1].status == st.status:
                continue

            # 2) Collapse short inactive→active flaps
            if len(out) >= 1:
                last = out[-1]
                if st.status == "active" and last.status == "inactive":
                    # If we bounced back to active quickly, remove the flap.
                    if st.at - last.at <= _FLAP_WINDOW:
                        # Remove the middle 'inactive' and skip appending current 'active'
                        out.pop()  # remove the 'inactive'
                        continue

        out.append(st)

    return out


class Job(BaseModel):
    # Uniquely identified by (title, link). In practice, the link should be unique.
    title: str = Field(..., min_length=1)
    link: str
    history: List[Status] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalize(self) -> "Job":
        self.history = _normalize_history(self.history)
        return self

    def is_active(self) -> bool:
        return bool(self.history) and self.history[-1].status == "active"

    def active_hours(self) -> float:
        # If not currently active, latest active streak length is 0
        if not self.is_active():
            return 0.0

        # Walk backwards until the most recent 'inactive' (the streak boundary)
        start: Optional[datetime] = None
        for st in reversed(self.history):
            if st.status == "inactive":
                break
            start = st.at  # earliest 'active' in the trailing active block

        if start is None:
            return 0.0  # defensive

        # Compute hours from the streak start until now (UTC)
        delta = now_utc() - start
        return max(0.0, delta.total_seconds() / 3600.0)

    def mark(self, new_status: Literal["active", "inactive"], at: Optional[datetime] = None) -> None:
        """
        Append a status-change event unless it would duplicate the last event.
        """
        ts = at or now_utc()
        if self.history and self.history[-1].status == new_status:
            # Rule: do not add event if last event has the same status
            return
        self.history.append(Status(status=new_status, at=ts))
        # Keep tidy immediately
        self.history = _normalize_history(self.history)


# ---------- scrape health & attempts ----------
class ScrapeAttempt(BaseModel):
    at: datetime = Field(default_factory=now_utc)
    ok: bool = True                     # request/render succeeded
    count: int = 0                      # len(scraped_jobs)
    duration_ms: Optional[int] = None   # optional timing
    renderer_used: Optional[bool] = None
    error_kind: Optional[str] = None    # e.g., "timeout", "403", "render-empty"


class ScrapePolicy(BaseModel):
    # Use raw seconds to keep JSON serialization simple.
    time_flag_duration_s: int = 24 * 3600          # 24h
    attempt_threshold_for_down: int = 5            # N zero attempts within window -> "down"
    attempt_window_size: int = 10                  # for baseline calc (successful attempts)
    min_baseline_to_flag: int = 3                  # only flag if historically >=3 jobs
    require_two_successful_zeros_to_deactivate: bool = True
    manual_override: bool = False                  # if True, health won't auto-clear


class ScrapeHealth(BaseModel):
    status: Literal["normal", "suspect", "down"] = "normal"
    reason: Literal["NONE", "ZERO_SPIKE", "EMPTY_STREAK", "MANUAL"] = "NONE"

    first_zero_at: Optional[datetime] = None
    consecutive_zero_attempts: int = 0
    flagged_until: Optional[datetime] = None

    # Last time and size of a non-zero result (useful baseline anchor)
    last_nonzero_at: Optional[datetime] = None
    last_nonzero_count: Optional[int] = None

    # Rolling baseline over recent successes (median or trimmed mean; we store median)
    baseline_nonzero_count: Optional[int] = None

    # Convenience markers for external display/alerts
    last_success_at: Optional[datetime] = None
    last_success_count: Optional[int] = None


def _median(ints: List[int]) -> Optional[int]:
    if not ints:
        return None
    s = sorted(ints)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    # average of two middles; return int
    return (s[mid - 1] + s[mid]) // 2


# ---------- JobBoard with health-aware merging ----------
class JobBoard(BaseModel):
    title: str = Field(..., min_length=1)
    icon_url: AnyUrl
    website_url: AnyUrl

    # Attempts & health
    attempts: List[ScrapeAttempt] = Field(default_factory=list)
    scrape_health: ScrapeHealth = Field(default_factory=ScrapeHealth)
    policy: ScrapePolicy = Field(default_factory=ScrapePolicy)

    # Timestamps
    last_scraped: Optional[datetime] = None        # last attempt time (successful or not)
    last_success_at: Optional[datetime] = None     # last attempt with count>0 and ok=True
    next_scrape_at: Optional[datetime] = None

    # Data
    content: List[Job] = Field(default_factory=list)

    # --- File IO ---
    @classmethod
    @ttl_cache(ttl=30)
    def from_file(cls, path: Path) -> "JobBoard":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(**data)

    def to_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        text = self.model_dump_json(indent=2)
        path.write_text(text, encoding="utf-8")

    # --- Health & attempts ---
    def record_attempt(
        self,
        scraped_jobs: List[Job],
        *,
        ok: bool = True,
        scraped_at: Optional[datetime] = None,
        duration_ms: Optional[int] = None,
        renderer_used: Optional[bool] = None,
        error_kind: Optional[str] = None,
    ) -> ScrapeHealth:
        """
        Record a scrape attempt, update health state (suspect/down/normal), and
        return the current ScrapeHealth. Does NOT mutate self.content.
        """
        ts = scraped_at or now_utc()
        cnt = len(scraped_jobs)
        self.last_scraped = ts

        # Append attempt (keep a modest cap to bound JSON size)
        self.attempts.append(
            ScrapeAttempt(
                at=ts, ok=ok, count=cnt,
                duration_ms=duration_ms,
                renderer_used=renderer_used,
                error_kind=error_kind,
            )
        )
        if len(self.attempts) > 50:
            self.attempts = self.attempts[-50:]

        health = self.scrape_health
        policy = self.policy
        window = timedelta(seconds=policy.time_flag_duration_s)

        # Auto-clear when manual override is set
        if policy.manual_override:
            health.status = "down" if health.status == "down" else "suspect"
            health.reason = "MANUAL"
            # Don't auto-update counters in manual mode beyond attempts list
            return health

        # SUCCESS path: cnt > 0 and ok
        if cnt > 0 and ok:
            health.consecutive_zero_attempts = 0
            health.first_zero_at = None
            health.flagged_until = None
            health.status = "normal"
            health.reason = "NONE"

            # Update success markers
            health.last_nonzero_at = ts
            health.last_nonzero_count = cnt
            health.last_success_at = ts
            health.last_success_count = cnt
            self.last_success_at = ts

            # Recompute baseline over last K successes
            k = policy.attempt_window_size
            successes = [a.count for a in reversed(self.attempts) if a.ok and a.count > 0]
            successes = successes[:k] if len(successes) > k else successes
            health.baseline_nonzero_count = _median(list(reversed(successes))) if successes else cnt
            return health

        # EMPTY / FAILED path: cnt == 0 OR !ok
        if health.consecutive_zero_attempts == 0:
            health.first_zero_at = ts
        health.consecutive_zero_attempts += 1

        # Reason
        prior_nonzero_exists = (
            (health.last_nonzero_count or 0) >= policy.min_baseline_to_flag
        )
        # Detect if the *previous* attempt had jobs (zero spike)
        prev = self.attempts[-2] if len(self.attempts) >= 2 else None
        if prev and prev.ok and prev.count > 0 and cnt == 0:
            health.reason = "ZERO_SPIKE"
        else:
            health.reason = "EMPTY_STREAK"

        # Apply time-based flag
        fu = ts + window
        health.flagged_until = fu if (health.flagged_until is None or fu > health.flagged_until) else health.flagged_until

        # Status bumping logic
        if prior_nonzero_exists:
            # If enough zero attempts in the window, mark as down
            within_window = (health.first_zero_at is not None) and (ts - health.first_zero_at <= window)
            if within_window and health.consecutive_zero_attempts >= policy.attempt_threshold_for_down:
                health.status = "down"
            else:
                health.status = "suspect"
        else:
            # No baseline yet; be conservative: go suspect only after 2+ zero attempts
            health.status = "suspect" if health.consecutive_zero_attempts >= 2 else "normal"

        return health

    def health_summary(self) -> Dict[str, object]:
        h = self.scrape_health
        return {
            "status": h.status,
            "reason": h.reason,
            "first_zero_at": h.first_zero_at,
            "consecutive_zero_attempts": h.consecutive_zero_attempts,
            "flagged_until": h.flagged_until,
            "last_nonzero_at": h.last_nonzero_at,
            "last_nonzero_count": h.last_nonzero_count,
            "baseline_nonzero_count": h.baseline_nonzero_count,
            "last_success_at": h.last_success_at,
            "last_success_count": h.last_success_count,
        }

    # --- Health-aware merge ---
    def apply_scrape(
        self,
        scraped_jobs: List[Job],
        scraped_at: Optional[datetime] = None,
        *,
        ok: bool = True,
        duration_ms: Optional[int] = None,
        renderer_used: Optional[bool] = None,
        error_kind: Optional[str] = None,
    ) -> None:
        """
        Merge 'currently active' scraped jobs into this page with health gating:
          - Record every attempt in 'attempts' and update 'scrape_health'
          - If attempt is zero and health is 'suspect' or 'down', SKIP mass deactivation
          - Optionally require two consecutive successful zeros to deactivate
          - On successful non-zero attempts, proceed with normal merge
        """
        ts = scraped_at or now_utc()
        health = self.record_attempt(
            scraped_jobs,
            ok=ok,
            scraped_at=ts,
            duration_ms=duration_ms,
            renderer_used=renderer_used,
            error_kind=error_kind,
        )

        count = len(scraped_jobs)
        # Gate zero-result merges
        if count == 0:
            safe_to_deactivate = False

            if self.policy.require_two_successful_zeros_to_deactivate:
                # Require current and previous attempt to be ok & zero, AND health not suspect/down
                prev = self.attempts[-2] if len(self.attempts) >= 2 else None
                if (
                    ok and
                    prev is not None and prev.ok and prev.count == 0 and
                    health.status == "normal"
                ):
                    safe_to_deactivate = True
            else:
                # Allow deactivation only if health is normal (not suspect/down)
                safe_to_deactivate = health.status == "normal"

            if not safe_to_deactivate:
                # Do not modify content; just keep last_scraped updated and return
                return

        # Proceed with normal merge/deactivation

        # Index current jobs by link (primary key)
        by_link: Dict[str, Job] = {str(j.link): j for j in self.content}
        scraped_by_link: Dict[str, Job] = {str(j.link): j for j in scraped_jobs}

        # Activate / upsert scraped jobs
        for link, new_job in scraped_by_link.items():
            if link in by_link:
                cur = by_link[link]
                # Update title if it changed
                if new_job.title and new_job.title != cur.title:
                    cur.title = new_job.title
                cur.mark("active", ts)  # no-op if already active
            else:
                # New job, mark as active now
                new_job.history = _normalize_history([Status(status="active", at=ts)])
                self.content.append(new_job)

        # Deactivate jobs missing from the scrape
        for link, existing in by_link.items():
            if link not in scraped_by_link and existing.is_active():
                existing.mark("inactive", ts)  # no-op if already inactive

        # Timestamps & success markers on non-zero merges
        if count > 0 and ok:
            self.last_success_at = ts
            self.scrape_health.last_success_at = ts
            self.scrape_health.last_success_count = count

        # Sort active first, then title
        self.content.sort(key=lambda j: (0 if j.is_active() else 1, j.title.lower()))


# ---------- utilities (unchanged) ----------
def slugify(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return value or "page"


def list_page_files(pages_dir: Path) -> list[Path]:
    pages_dir.mkdir(parents=True, exist_ok=True)
    return sorted(p for p in pages_dir.glob("*.json") if p.is_file())


def load_pages(pages_dir: Path) -> list[Tuple[Path, JobBoard]]:
    out: list[Tuple[Path, JobBoard]] = []
    for jf in list_page_files(pages_dir):
        try:
            out.append((jf, JobBoard.from_file(jf)))
        except ValidationError:
            # Skip invalid JSONs; you could log these if you want.
            continue
    return out
