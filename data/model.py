from __future__ import annotations
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Literal, Dict
from pydantic import BaseModel, AnyUrl, Field, ValidationError, field_validator


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class Status(BaseModel):
    status: Literal["active", "inactive"]
    # Timestamp (UTC) of when this status was observed/changed
    at: datetime = Field(default_factory=now_utc)


class Job(BaseModel):
    # Uniquely identified by (title, link). In practice, the link should be unique.
    title: str = Field(..., min_length=1)
    link: AnyUrl
    history: List[Status] = Field(default_factory=list)

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
            return 0.0  # defensive: shouldn't happen if is_active() is True

        # Compute hours from the streak start until now (UTC)
        delta = now_utc() - start
        return max(0.0, delta.total_seconds() / 3600.0)

    def mark(self, new_status: Literal["active", "inactive"], at: Optional[datetime] = None) -> None:
        self.history.append(Status(status=new_status, at=at or now_utc()))


class JobBoard(BaseModel):
    title: str = Field(..., min_length=1)
    icon_url: AnyUrl
    website_url: AnyUrl
    last_scraped: Optional[datetime] = None
    next_scrape_at: Optional[datetime] = None
    content: List[Job] = Field(default_factory=list)

    # --- File IO ---
    @classmethod
    def from_file(cls, path: Path) -> "JobBoard":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(**data)

    def to_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            text = self.model_dump_json(indent=2)  # pydantic v2
        except AttributeError:
            text = self.json(indent=2)             # pydantic v1 fallback
        path.write_text(text, encoding="utf-8")

    # --- Helpers for maintaining history ---
    def apply_scrape(self, scraped_jobs: List[Job], scraped_at: Optional[datetime] = None) -> None:
        """
        Merge 'currently active' scraped jobs into this page:
          - Jobs present in scrape -> ensure final status is 'active'
          - Jobs missing from scrape but were active -> add 'inactive'
          - Preserve history
        """
        ts = scraped_at or now_utc()

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
                cur.mark("active", ts)
            else:
                # New job, mark as active now
                new_job.history = [Status(status="active", at=ts)]
                self.content.append(new_job)

        # Deactivate jobs missing from the scrape
        for link, existing in by_link.items():
            if link not in scraped_by_link and existing.is_active():
                existing.mark("inactive", ts)

        # Update timestamp
        self.last_scraped = ts

        # Optional: sort active first, then title
        self.content.sort(key=lambda j: (0 if j.is_active() else 1, j.title.lower()))


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
