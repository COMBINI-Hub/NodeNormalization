#!/usr/bin/env python3
"""Fetch MeSH identifiers for PubChem CIDs via PubChem PUG-View (batched safely).

Why this exists
---------------
NodeNormalization often returns sparse MeSH cross-references for PubChem compounds.
This script uses PubChem's PUG-View service to retrieve MeSH-related annotations
for a *sample* of PubChem CIDs, with:
- strict request rate limiting (default < 5 req/sec)
- retries with backoff for transient errors
- sqlite cache to avoid re-fetching

Important limitation
--------------------
PUG-View does not support multiple CIDs per request, so this script is
intentionally optimized for small random samples (or for incremental runs using
cache), not for crawling the full PubChem database.

By default, this script targets the PUG-View heading:
    "MeSH Pharmacological Classification"

It can also fetch the full PUG-View record (heavier per CID) and extract MeSH
descriptor IDs from sections whose TOCHeading contains "MeSH". This avoids common
false positives where strings like "D001241" appear in unrelated contexts.

Outputs
-------
Writes a TSV with one row per input CID:
  cid, mesh_ids, status, source

where mesh_ids is a pipe-separated list of MESH:... identifiers.

Example
-------
python pubchem_mesh_from_pug_view.py \
  --input all_ikraph_pubchem_ids.txt \
  --sample-size 1000 \
  --seed 123 \
  --out pubchem_mesh_sample.tsv
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


PUG_VIEW_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view"
DEFAULT_HEADING = "MeSH Pharmacological Classification"


_MESH_ID_PATTERN = re.compile(r"\b([DC]\d{6})\b")
_MESH_UI_PATTERN = re.compile(r"^([DC]\d{6})$")
_MESH_REF_ID_PATTERN = re.compile(r"^([DC]\d{6}|M\d{7})$")


@dataclass(frozen=True)
class FetchResult:
    cid: int
    mesh_ids: Tuple[str, ...]
    status: str  # ok | none | error
    source: str
    error: Optional[str] = None


class RateLimiter:
    def __init__(self, max_rps: float) -> None:
        if max_rps <= 0:
            raise ValueError("max_rps must be > 0")
        self._min_interval = 1.0 / max_rps
        self._last_time: Optional[float] = None

    def wait(self) -> None:
        now = time.monotonic()
        if self._last_time is None:
            self._last_time = now
            return
        elapsed = now - self._last_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_time = time.monotonic()


def reservoir_sample_ints(lines: Iterable[str], k: int, seed: Optional[int]) -> List[int]:
    """Reservoir-sample up to k integers from an iterator of lines."""
    rng = random.Random(seed)
    reservoir: List[int] = []
    seen = 0

    for raw in lines:
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        try:
            value = int(s)
        except ValueError:
            continue

        seen += 1
        if len(reservoir) < k:
            reservoir.append(value)
        else:
            j = rng.randrange(seen)
            if j < k:
                reservoir[j] = value

    return reservoir


def open_cache(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    # v3 schema uses a composite key so we can safely cache:
    # - both the light heading-based fetches and the full-record fetches
    # - both extraction methods (fixed-field vs regex)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pug_view_mesh_v3 (
            cid INTEGER NOT NULL,
            mode TEXT NOT NULL,
            heading TEXT NOT NULL,
            extract_method TEXT NOT NULL,
            mesh_ids_json TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT NOT NULL,
            error TEXT,
            fetched_at_utc TEXT NOT NULL,
            PRIMARY KEY (cid, mode, heading, extract_method)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pug_view_mesh_v3_heading ON pug_view_mesh_v3(heading)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pug_view_mesh_v3_mode ON pug_view_mesh_v3(mode)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pug_view_mesh_v3_extract_method ON pug_view_mesh_v3(extract_method)"
    )
    conn.commit()
    return conn


def cache_get(
    conn: sqlite3.Connection,
    cid: int,
    *,
    mode: str,
    heading: str,
    extract_method: str,
) -> Optional[FetchResult]:
    row = conn.execute(
        "SELECT mesh_ids_json, status, source, error FROM pug_view_mesh_v3 WHERE cid = ? AND mode = ? AND heading = ? AND extract_method = ?",
        (cid, mode, heading, extract_method),
    ).fetchone()
    if not row:
        return None
    mesh_ids_json, status, source, error = row
    try:
        mesh_ids_list = json.loads(mesh_ids_json)
        if not isinstance(mesh_ids_list, list):
            mesh_ids_list = []
    except Exception:
        mesh_ids_list = []
    mesh_ids = tuple(str(x) for x in mesh_ids_list)
    return FetchResult(cid=cid, mesh_ids=mesh_ids, status=status, source=source, error=error)


def cache_put(
    conn: sqlite3.Connection,
    result: FetchResult,
    *,
    mode: str,
    heading: str,
    extract_method: str,
) -> None:
    fetched_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO pug_view_mesh_v3 (cid, mode, heading, extract_method, mesh_ids_json, status, source, error, fetched_at_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cid, mode, heading, extract_method) DO UPDATE SET
            mesh_ids_json=excluded.mesh_ids_json,
            status=excluded.status,
            source=excluded.source,
            error=excluded.error,
            fetched_at_utc=excluded.fetched_at_utc
        """,
        (
            result.cid,
            mode,
            heading,
            extract_method,
            json.dumps(list(result.mesh_ids), sort_keys=True),
            result.status,
            result.source,
            result.error,
            fetched_at_utc,
        ),
    )


def build_pug_view_heading_url(cid: int, heading: str) -> str:
    # PUG-View accepts either '+' or %20 for spaces; urllib.parse.quote uses %20.
    return f"{PUG_VIEW_BASE}/data/compound/{cid}/JSON?heading={urllib.parse.quote(heading)}"


def http_get_json(url: str, timeout_s: int) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.load(resp)  # type: ignore[no-any-return]


def _iter_mesh_sections(payload: object) -> Iterable[dict]:
    """Yield section-like dict objects whose TOCHeading contains 'MeSH'."""
    if isinstance(payload, dict):
        toc = payload.get("TOCHeading")
        if isinstance(toc, str) and "mesh" in toc.lower():
            yield payload
        for v in payload.values():
            yield from _iter_mesh_sections(v)
    elif isinstance(payload, list):
        for v in payload:
            yield from _iter_mesh_sections(v)


def _extract_mesh_ids_from_url(url: str) -> Iterable[str]:
    """Extract MeSH IDs from a MeSH-related URL.

    Supported:
    - Descriptor UI: D######
    - Supplementary concept UI: C######
    - NLM MeSH record IDs exposed by PUG-View references: M#######
    """
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return

    # e.g. https://meshb.nlm.nih.gov/record/ui?ui=D020228
    try:
        qs = urllib.parse.parse_qs(parsed.query)
        ui_vals = qs.get("ui") or []
        for ui in ui_vals:
            if isinstance(ui, str) and _MESH_REF_ID_PATTERN.match(ui):
                yield ui
    except Exception:
        pass

    # e.g. https://id.nlm.nih.gov/mesh/D012345.html or https://id.nlm.nih.gov/mesh/M0011721.html
    path = parsed.path or ""
    if "/mesh/" in path:
        tail = path.rsplit("/", 1)[-1]
        if tail.endswith(".html"):
            tail = tail[: -len(".html")]
        if _MESH_REF_ID_PATTERN.match(tail):
            yield tail


def _iter_mesh_ids_from_fixed_fields(obj: object) -> Iterable[str]:
    """Yield MeSH IDs by following stable JSON fields (URLs, IDs), not regexing blobs."""
    if isinstance(obj, dict):
        toc = obj.get("TOCHeading")
        skip_section_url = isinstance(toc, str) and toc.strip() == "MeSH Pharmacological Classification"
        for k, v in obj.items():
            if k == "URL" and isinstance(v, str):
                if skip_section_url:
                    # This heading's section-level URL is a constant MeSH page and
                    # is not the specific pharmacological class identifier.
                    continue
                yield from _extract_mesh_ids_from_url(v)
            elif k == "SourceID" and isinstance(v, str) and _MESH_REF_ID_PATTERN.match(v):
                yield v
            else:
                yield from _iter_mesh_ids_from_fixed_fields(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_mesh_ids_from_fixed_fields(v)


def _iter_mesh_ids_from_regex(obj: object) -> Iterable[str]:
    if isinstance(obj, dict):
        blob = json.dumps(obj, ensure_ascii=False)
        yield from (m.group(1) for m in _MESH_ID_PATTERN.finditer(blob))
    else:
        blob = json.dumps(obj, ensure_ascii=False)
        yield from (m.group(1) for m in _MESH_ID_PATTERN.finditer(blob))


def extract_mesh_ids_from_pug_view_payload(
    payload: dict,
    *,
    restrict_to_mesh_sections: bool,
    extract_method: str,
) -> Tuple[str, ...]:
    """Extract MeSH IDs from a PUG-View JSON payload.

    - extract_method='fixed': follow stable fields for D/C only (legacy).
    - extract_method='fixed-plus-m': follow stable fields and also include MeSH M-record IDs from references.
    - extract_method='regex': legacy behavior; scans JSON text for D######/C######.

    If restrict_to_mesh_sections=True, only inspect portions of the payload that are
    in sections with a TOCHeading containing 'MeSH'.
    """
    if extract_method not in {"fixed", "fixed-plus-m", "regex"}:
        raise ValueError(f"Unsupported extract_method: {extract_method}")

    sources: Iterable[object]
    if restrict_to_mesh_sections:
        sources = _iter_mesh_sections(payload)
    else:
        sources = (payload,)

    raw_ids: set[str] = set()
    if extract_method in {"fixed", "fixed-plus-m"}:
        for section in sources:
            raw_ids.update(_iter_mesh_ids_from_fixed_fields(section))
        if extract_method == "fixed":
            # Back-compat: only D/C IDs.
            raw_ids = {rid for rid in raw_ids if _MESH_UI_PATTERN.match(rid)}
    else:
        for section in sources:
            raw_ids.update(_iter_mesh_ids_from_regex(section))

    return tuple(sorted({f"MESH:{rid}" for rid in raw_ids}))


def build_pug_view_full_url(cid: int) -> str:
    return f"{PUG_VIEW_BASE}/data/compound/{cid}/JSON"


def fetch_mesh_for_cid(
    cid: int,
    *,
    mode: str,
    heading: str,
    extract_method: str,
    timeout_s: int,
    retries: int,
    limiter: RateLimiter,
) -> FetchResult:
    if mode == "heading":
        url = build_pug_view_heading_url(cid, heading)
        restrict = False
    elif mode == "full":
        url = build_pug_view_full_url(cid)
        restrict = True
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    last_err: Optional[str] = None
    for attempt in range(retries + 1):
        limiter.wait()
        try:
            payload = http_get_json(url, timeout_s=timeout_s)
            mesh_ids = extract_mesh_ids_from_pug_view_payload(
                payload,
                restrict_to_mesh_sections=restrict,
                extract_method=extract_method,
            )
            if mesh_ids:
                return FetchResult(cid=cid, mesh_ids=mesh_ids, status="ok", source="pug_view")
            return FetchResult(cid=cid, mesh_ids=(), status="none", source="pug_view")
        except urllib.error.HTTPError as e:
            code = getattr(e, "code", None)
            # 404 (NotFound) and 400 (BadRequest) usually mean the heading isn't present.
            if code in (400, 404):
                return FetchResult(cid=cid, mesh_ids=(), status="none", source="pug_view", error=f"HTTP {code}")

            # For throttling / transient issues, retry.
            if code in (429, 500, 502, 503, 504):
                retry_after = e.headers.get("Retry-After") if hasattr(e, "headers") else None
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except Exception:
                        pass
                else:
                    time.sleep(min(60.0, (2.0 ** attempt)))
                last_err = f"HTTP {code}"
                continue

            # Other HTTP codes: do not loop too much.
            last_err = f"HTTP {code}"
            break
        except Exception as e:
            # network errors, json errors, etc.
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(min(60.0, (2.0 ** attempt)))

    return FetchResult(
        cid=cid,
        mesh_ids=(),
        status="error",
        source="pug_view",
        error=last_err,
    )


def write_tsv(path: Path, rows: Sequence[FetchResult]) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write("cid\tmesh_ids\tstatus\tsource\terror\n")
        for r in rows:
            mesh = "|".join(r.mesh_ids)
            err = r.error or ""
            f.write(f"{r.cid}\t{mesh}\t{r.status}\t{r.source}\t{err}\n")


def iter_cids_from_file(path: Path) -> Iterable[int]:
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            try:
                yield int(s)
            except ValueError:
                continue


def open_tsv_for_incremental_write(path: Path) -> tuple[object, bool]:
    """Open TSV for appending and return (file_handle, wrote_header?)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    existed = path.exists()
    f = path.open("a", encoding="utf-8")
    wrote_header = False
    if not existed or path.stat().st_size == 0:
        f.write("cid\tmesh_ids\tstatus\tsource\terror\n")
        wrote_header = True
        f.flush()
    return f, wrote_header


@dataclass
class RunCheckpoint:
    offset: int
    processed: int
    ok: int
    none: int
    error: int
    cached: int


def checkpoint_key(*, input_path: Path, mode: str, heading: str, extract_method: str) -> str:
    return f"{input_path.resolve()}|{mode}|{heading}|{extract_method}"


def load_checkpoint(path: Path, *, key: str) -> Optional[RunCheckpoint]:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        entry = data.get(key)
        if not isinstance(entry, dict):
            return None
        return RunCheckpoint(
            offset=int(entry.get("offset", 0)),
            processed=int(entry.get("processed", 0)),
            ok=int(entry.get("ok", 0)),
            none=int(entry.get("none", 0)),
            error=int(entry.get("error", 0)),
            cached=int(entry.get("cached", 0)),
        )
    except Exception:
        return None


def save_checkpoint(path: Path, *, key: str, ckpt: RunCheckpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}

    data[key] = {
        "offset": ckpt.offset,
        "processed": ckpt.processed,
        "ok": ckpt.ok,
        "none": ckpt.none,
        "error": ckpt.error,
        "cached": ckpt.cached,
        "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def iter_cids_from_file_with_checkpoint(
    path: Path,
    *,
    start_offset: int,
) -> Iterable[tuple[int, int]]:
    """Yield (cid, next_offset) from a newline-delimited file, in binary mode.

    Offsets are byte offsets suitable for seek()/tell().
    """
    with path.open("rb") as f:
        if start_offset:
            f.seek(start_offset)
        while True:
            line = f.readline()
            if not line:
                break
            next_offset = f.tell()
            try:
                s = line.decode("utf-8", errors="ignore").strip()
            except Exception:
                continue
            if not s or s.startswith("#"):
                continue
            try:
                cid = int(s)
            except ValueError:
                continue
            yield cid, next_offset


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch MeSH IDs for PubChem CIDs via PubChem PUG-View")
    ap.add_argument(
        "--input",
        type=Path,
        default=Path("all_ikraph_pubchem_ids.txt"),
        help="Input file with PubChem CIDs (one per line)",
    )
    ap.add_argument(
        "--sample-size",
        type=int,
        default=200,
        help="Number of random CIDs to sample from input (default: 200)",
    )
    ap.add_argument(
        "--process-all",
        action="store_true",
        help="Process all CIDs from --input (streaming) instead of sampling",
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for sampling (default: none)",
    )
    ap.add_argument(
        "--mode",
        choices=["heading", "full"],
        default="heading",
        help="Fetch mode: 'heading' (light) or 'full' (heavier; extracts from MeSH-labeled sections)",
    )
    ap.add_argument(
        "--extract-method",
        choices=["fixed", "fixed-plus-m", "regex"],
        default="fixed",
        help=(
            "Extraction method: "
            "'fixed' follows stable JSON fields but keeps only D/C; "
            "'fixed-plus-m' also includes MeSH M####### reference IDs; "
            "'regex' scans JSON text for D######/C######"
        ),
    )
    ap.add_argument(
        "--heading",
        type=str,
        default=DEFAULT_HEADING,
        help=f"PUG-View heading to fetch (default: {DEFAULT_HEADING!r})",
    )
    ap.add_argument(
        "--max-rps",
        type=float,
        default=3.0,
        help="Max requests/sec (default: 3.0; PubChem asks <= 5)",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout seconds (default: 30)",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry count for transient failures (default: 3)",
    )
    ap.add_argument(
        "--cache",
        type=Path,
        default=Path(".cache/pubchem_mesh_pug_view.sqlite"),
        help="SQLite cache path (default: .cache/pubchem_mesh_pug_view.sqlite)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("pubchem_mesh_sample.tsv"),
        help="Output TSV path (default: pubchem_mesh_sample.tsv)",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Print progress every N processed CIDs (default: 1000)",
    )
    ap.add_argument(
        "--commit-every",
        type=int,
        default=250,
        help="Commit sqlite cache every N processed CIDs (default: 250)",
    )
    ap.add_argument(
        "--checkpoint",
        type=Path,
        default=Path(".cache/pubchem_mesh_pug_view.checkpoint.json"),
        help="Checkpoint JSON path for resumable --process-all runs (default: .cache/pubchem_mesh_pug_view.checkpoint.json)",
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="Resume a previous --process-all run using --checkpoint (continues from last input byte offset)",
    )
    ap.add_argument(
        "--stop-after",
        type=int,
        default=0,
        help="Stop after processing N CIDs (0 means no limit; useful for smoke tests)",
    )
    ap.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cache and re-fetch all sampled CIDs",
    )
    args = ap.parse_args()

    if not args.process_all and args.sample_size <= 0:
        print("Error: --sample-size must be > 0 (unless --process-all)", file=sys.stderr)
        return 2

    if not args.input.exists():
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        return 2

    limiter = RateLimiter(max_rps=args.max_rps)

    if args.process_all:
        total_hint: Optional[int] = None
        ckpt_key = checkpoint_key(
            input_path=args.input,
            mode=args.mode,
            heading=args.heading,
            extract_method=args.extract_method,
        )
        start_offset = 0
        starting_counts: Optional[RunCheckpoint] = None
        if args.resume:
            starting_counts = load_checkpoint(args.checkpoint, key=ckpt_key)
            if starting_counts is not None:
                start_offset = max(0, int(starting_counts.offset))
        cids_iter = iter_cids_from_file_with_checkpoint(args.input, start_offset=start_offset)
    else:
        with args.input.open("r", encoding="utf-8") as f:
            sample_cids = reservoir_sample_ints(f, k=args.sample_size, seed=args.seed)

        if not sample_cids:
            print("Error: no CIDs sampled from input", file=sys.stderr)
            return 2

        # Shuffle to avoid any subtle ordering bias from reservoir sampling.
        rng = random.Random(args.seed)
        rng.shuffle(sample_cids)
        cids_iter = sample_cids
        total_hint = len(sample_cids)

    conn = open_cache(args.cache)
    try:
        out_f, _ = open_tsv_for_incremental_write(args.out)
        if args.process_all and args.resume and starting_counts is not None:
            processed = starting_counts.processed
            ok = starting_counts.ok
            none = starting_counts.none
            err = starting_counts.error
            cached = starting_counts.cached
            current_offset = starting_counts.offset
        else:
            processed = 0
            ok = 0
            none = 0
            err = 0
            cached = 0
            current_offset = 0
        processed_this_run = 0
        try:
            for item in cids_iter:
                if args.stop_after and processed_this_run >= args.stop_after:
                    break

                if args.process_all:
                    cid, next_offset = item  # type: ignore[misc]
                else:
                    cid = item  # type: ignore[assignment]
                    next_offset = current_offset

                processed += 1

                if not args.force_refresh:
                    hit = cache_get(
                        conn,
                        cid,
                        mode=args.mode,
                        heading=args.heading,
                        extract_method=args.extract_method,
                    )
                    if hit is not None:
                        cached += 1
                        r = hit
                    else:
                        r = fetch_mesh_for_cid(
                            cid,
                            mode=args.mode,
                            heading=args.heading,
                            extract_method=args.extract_method,
                            timeout_s=args.timeout,
                            retries=args.retries,
                            limiter=limiter,
                        )
                        cache_put(conn, r, mode=args.mode, heading=args.heading, extract_method=args.extract_method)
                else:
                    r = fetch_mesh_for_cid(
                        cid,
                        mode=args.mode,
                        heading=args.heading,
                        extract_method=args.extract_method,
                        timeout_s=args.timeout,
                        retries=args.retries,
                        limiter=limiter,
                    )
                    cache_put(conn, r, mode=args.mode, heading=args.heading, extract_method=args.extract_method)

                if r.status == "ok":
                    ok += 1
                elif r.status == "none":
                    none += 1
                else:
                    err += 1

                mesh = "|".join(r.mesh_ids)
                err_msg = r.error or ""
                out_f.write(f"{r.cid}\t{mesh}\t{r.status}\t{r.source}\t{err_msg}\n")
                current_offset = next_offset
                processed_this_run += 1

                if args.commit_every > 0 and processed % args.commit_every == 0:
                    conn.commit()
                    out_f.flush()
                    if args.process_all:
                        save_checkpoint(
                            args.checkpoint,
                            key=ckpt_key,
                            ckpt=RunCheckpoint(
                                offset=current_offset,
                                processed=processed,
                                ok=ok,
                                none=none,
                                error=err,
                                cached=cached,
                            ),
                        )

                if args.progress_every > 0 and processed % args.progress_every == 0:
                    denom = total_hint if total_hint is not None else processed
                    print(
                        f"Progress {processed}/{denom} | ok={ok} none={none} error={err} cached={cached}",
                        file=sys.stderr,
                    )
                    if args.process_all:
                        save_checkpoint(
                            args.checkpoint,
                            key=ckpt_key,
                            ckpt=RunCheckpoint(
                                offset=current_offset,
                                processed=processed,
                                ok=ok,
                                none=none,
                                error=err,
                                cached=cached,
                            ),
                        )

            conn.commit()
            out_f.flush()
            if args.process_all:
                save_checkpoint(
                    args.checkpoint,
                    key=ckpt_key,
                    ckpt=RunCheckpoint(
                        offset=current_offset,
                        processed=processed,
                        ok=ok,
                        none=none,
                        error=err,
                        cached=cached,
                    ),
                )
        finally:
            out_f.close()

    finally:
        conn.close()

    unique_mesh_note = "(not tracked in streaming mode)" if args.process_all else ""

    print("=" * 70, file=sys.stderr)
    print("PUG-VIEW MeSH SAMPLE RESULTS", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"Input: {args.input}", file=sys.stderr)
    if args.process_all:
        print("Mode: process-all", file=sys.stderr)
    else:
        print(f"Sample size: {total_hint:,}", file=sys.stderr)
    print(f"Mode: {args.mode}", file=sys.stderr)
    print(f"Heading: {args.heading}", file=sys.stderr)
    print(f"Extract method: {args.extract_method}", file=sys.stderr)
    print(f"Cache: {args.cache}", file=sys.stderr)
    print(f"Output TSV: {args.out}", file=sys.stderr)
    print("", file=sys.stderr)
    denom = processed if args.process_all else (total_hint or 1)
    print(f"Processed:    {processed:,}", file=sys.stderr)
    print(f"With MeSH IDs: {ok:,} ({ok/denom*100:.1f}%)", file=sys.stderr)
    print(f"No MeSH IDs:  {none:,} ({none/denom*100:.1f}%)", file=sys.stderr)
    print(f"Errors:       {err:,} ({err/denom*100:.1f}%)", file=sys.stderr)
    print(f"Unique MeSH IDs observed: {unique_mesh_note}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
