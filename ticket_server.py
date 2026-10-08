"""
Hermes ⇄ ST Ticket Server — MVC

A tiny server that accepts tickets (instruction strings), runs them through
`hermes chat` as persistent sessions via `podman exec`, one at a time off a
queue, and exposes status/result/trace over HTTP.

Run (real):
    XDG_RUNTIME_DIR=/run/user/1000 python3 ticket_server.py

Run (local mock test):
    HERMES_MODE=mock python3 ticket_server.py

Config via env:
    TICKET_DB        path to sqlite file        (default ./tickets.db)
    TICKET_HOST      bind host                  (default 127.0.0.1)
    TICKET_PORT      bind port                  (default 8002)
    TICKET_CORS_ORIGINS  comma-sep ST origins allowed to call this
                         (default http://127.0.0.1:8000,http://localhost:8000;
                          set to your Tailscale ST origin for phone access)
    HERMES_MODE      "real" | "mock"            (default real)
    HERMES_CONTAINER podman container name      (default hermes-agent)
    HERMES_TIMEOUT   seconds per ticket         (default 2700)
    TICKET_TOOLSETS  toolsets allowed in ticket runs (`-t`), default
                     web,terminal,file,todo — excludes all self-modifying
                     surfaces (skills/cronjob/messaging/delegation/...)
    TICKET_SKILLS    skills preloaded into ticket runs (`-s`), default none
    TICKET_MAX_TURNS max tool iterations per turn (`--max-turns`), default 40
"""

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# ---------------------------------------------------------------- config
DB_PATH = os.environ.get("TICKET_DB", "./tickets.db")
HOST = os.environ.get("TICKET_HOST", "127.0.0.1")
PORT = int(os.environ.get("TICKET_PORT", "8002"))
MODE = os.environ.get("HERMES_MODE", "real")
CONTAINER = os.environ.get("HERMES_CONTAINER", "hermes-agent")
TIMEOUT = int(os.environ.get("HERMES_TIMEOUT", "2700"))

# --- Per-invocation lockdown (T4) ---------------------------------------
# --yolo auto-approves whatever tools the session has; these make sure a
# ticket session only HAS a narrow, non-self-modifying set. Enforcement at
# the invocation boundary means global `hermes tools` toggles stay free for
# interactive use without affecting ticket safety.
#
# TICKET_TOOLSETS  comma-sep toolsets passed via `-t` (empty = hermes default
#                  set — NOT recommended). Deliberately excludes: skills
#                  (skill_manage self-editing), cronjob (self-scheduling),
#                  messaging, delegation, computer_use, browser.
# TICKET_SKILLS    comma-sep skills preloaded via `-s` (content injection,
#                  no skills toolset needed). Set to your searxng skill.
# TICKET_MAX_TURNS cap on tool-calling iterations per turn (hermes default
#                  is 90; runaway-loop damage is bounded by this).
TICKET_TOOLSETS = os.environ.get("TICKET_TOOLSETS", "web,terminal,file,todo").strip()
TICKET_SKILLS = os.environ.get("TICKET_SKILLS", "").strip()
TICKET_MAX_TURNS = os.environ.get("TICKET_MAX_TURNS", "40").strip()

# --- State bridge (SES nervous system) --------------------------------
# The shared nervous system lives INSIDE the hermes container at
# STATE_DIR (default /opt/data/shared/companion_state) — a podman volume the
# host cannot see directly. Phase 1 reads it through `podman exec cat`
# (same proven exec plumbing as the ticket runner). If the state dir is ever
# bind-mounted onto the host, set STATE_DIR_HOST and the routes switch
# to native file reads — no other code changes.
STATE_CONTAINER = os.environ.get("STATE_CONTAINER", "hermes-agent")
STATE_DIR = os.environ.get("STATE_DIR", "/opt/data/shared/companion_state")
STATE_DIR_HOST = os.environ.get("STATE_DIR_HOST", "").strip()
STATE_EXEC_TIMEOUT = int(os.environ.get("STATE_EXEC_TIMEOUT", "10"))

# SES master switch. Set SES_ENABLED=0 for a tickets-only server: /state
# serves an empty block, /experience is refused (no drops, no reconcile
# wakes), and nothing ever invokes the agent on the nervous system's
# behalf. Outbox and shelf need no switch — they simply report empty when
# their directories don't exist. The ticket routes are always on.
SES_ENABLED = os.environ.get("SES_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")

# Shelf (Phase 4): the Hands' own writing — curiosities (kind "hands") + outbox
# gifts (kind "gift") — indexed into chat memory by memory-pipeline's shelf-ingest
# pass. This server only READS shelf files; the Hands author them directly.
IDENTITY_DIR = os.environ.get("IDENTITY_DIR", "/opt/data/shared/companion_identity")
IDENTITY_DIR_HOST = os.environ.get("IDENTITY_DIR_HOST", "").strip()

# Compact rendering abbreviations for the state block (order = display order).
STATE_ABBR = [
    ("dopamine", "DA"),
    ("serotonin", "5HT"),
    ("oxytocin", "OXY"),
    ("norepinephrine", "NE"),
    ("cortisol", "CORT"),
    ("testosterone", "T"),
    ("estrogen", "E"),
    ("endocannabinoid", "eCB"),
]

# Origins allowed to call this server from a browser. The extension's fetch() runs
# in the ST browser, so when ST is loaded over Tailscale the request is cross-origin
# (ST on :8000, this server on :8002 = different origin) and the browser enforces CORS.
# Set this to the ST origin you actually load from, e.g.:
#   TICKET_CORS_ORIGINS="http://100.x.y.z:8000"
# Comma-separated for multiple (e.g. both the loopback PC origin and the Tailscale one).
# Default covers only local-loopback ST; the phone case REQUIRES setting the Tailscale origin.
CORS_ORIGINS = [
    o.strip() for o in os.environ.get(
        "TICKET_CORS_ORIGINS", "http://127.0.0.1:8000,http://localhost:8000"
    ).split(",") if o.strip()
]

SESSION_RE = re.compile(r"^session_id:\s*(\S+)\s*$")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- storage
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                instruction TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'open',
                result      TEXT,
                trace       TEXT,
                session_id  TEXT,
                delivered   INTEGER NOT NULL DEFAULT 0,
                parent_id   INTEGER,
                created_at  TEXT NOT NULL,
                started_at  TEXT,
                finished_at TEXT
            )
            """
        )
        # Migration: add columns to dbs created before they existed.
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(tickets)")}
        if "delivered" not in cols:
            conn.execute("ALTER TABLE tickets ADD COLUMN delivered INTEGER NOT NULL DEFAULT 0")
        if "parent_id" not in cols:
            conn.execute("ALTER TABLE tickets ADD COLUMN parent_id INTEGER")


def create_ticket(instruction: str, parent_id: int | None = None, delivered: bool = False) -> int:
    # delivered=True = internal ticket: runs normally but never surfaces via
    # /tickets/check (used by the reconcile wake so results don't spam the Voice).
    with db() as conn:
        if delivered:
            cur = conn.execute(
                "INSERT INTO tickets (instruction, status, created_at, parent_id, delivered) VALUES (?, 'open', ?, ?, 1)",
                (instruction, now(), parent_id),
            )
        else:
            cur = conn.execute(
                "INSERT INTO tickets (instruction, status, created_at, parent_id) VALUES (?, 'open', ?, ?)",
                (instruction, now(), parent_id),
            )
        return cur.lastrowid


def set_status(ticket_id: int, status: str, **fields) -> None:
    cols = ["status = ?"]
    vals = [status]
    for k, v in fields.items():
        cols.append(f"{k} = ?")
        vals.append(v)
    vals.append(ticket_id)
    with db() as conn:
        conn.execute(f"UPDATE tickets SET {', '.join(cols)} WHERE id = ?", vals)


def get_ticket(ticket_id: int):
    with db() as conn:
        row = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()
    return dict(row) if row else None


def list_tickets():
    with db() as conn:
        rows = conn.execute("SELECT * FROM tickets ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def list_undelivered():
    """Terminal (done/error) tickets the assistant hasn't acked yet.

    This is the cheap db-read the ST assistant polls every message. It is
    intentionally NARROW: only finished work that hasn't been surfaced.
    open/running tickets are excluded — there's nothing to deliver yet.
    """
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM tickets WHERE status IN ('done','error') AND delivered = 0 ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def mark_delivered(ticket_id: int) -> bool:
    """Returns True if a row was updated (ticket exists), False otherwise."""
    with db() as conn:
        cur = conn.execute("UPDATE tickets SET delivered = 1 WHERE id = ?", (ticket_id,))
        return cur.rowcount > 0


# ---------------------------------------------------------------- hermes runner
def build_cmd(instruction: str, resume: str | None = None) -> list[str]:
    """The proven invocation. Stateful session, quiet/programmatic, headless.

    When `resume` is set, continues an existing hermes session via --resume,
    so a follow-up ticket runs with the parent's full conversational context.
    """
    hermes = [
        "hermes", "chat",
        "-q", instruction,
        "-Q",
        "--yolo",
        "--source", "tool",
    ]
    # Lockdown flags (see config block). Applied on EVERY invocation,
    # including --resume follow-ups — the allowlist is per-session-run,
    # not inherited from the parent.
    if TICKET_TOOLSETS:
        hermes += ["-t", TICKET_TOOLSETS]
    if TICKET_SKILLS:
        hermes += ["-s", TICKET_SKILLS]
    if TICKET_MAX_TURNS:
        hermes += ["--max-turns", TICKET_MAX_TURNS]
    if resume:
        hermes += ["--resume", resume]
    if MODE == "mock":
        # Emit the exact shape real hermes does: final text line(s) then session_id.
        # On --resume, real hermes keeps the SAME session id, so the mock does too.
        sid = resume or datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        tag = "resumed" if resume else "answer"
        script = (
            "import sys,time;"
            "sys.stderr.write('[mock] thinking...\\n');"
            "time.sleep(2);"
            f"print('mock {tag} for: {instruction[:40]}');"
            f"sys.stderr.write('session_id: {sid}\\n')"
        )
        return ["python3", "-c", script]
    return ["podman", "exec", CONTAINER, *hermes]


def find_session_id(*streams: str) -> tuple[str | None, set[int]]:
    """Scan the given text streams for the LAST `session_id:` line.

    Real hermes (this build) writes the answer to stdout and the
    `session_id:` line to stderr, so we search both. Returns the id and
    the set of line-indices (within the FIRST stream only) that matched,
    so the caller can strip them from result text if needed.
    """
    session_id = None
    for stream in streams:
        for line in stream.splitlines():
            m = SESSION_RE.match(line)
            if m:
                session_id = m.group(1)  # keep last match across all streams
    # indices to strip from stdout specifically (in case it ever appears there)
    strip_idx = {
        i for i, line in enumerate(streams[0].splitlines())
        if SESSION_RE.match(line)
    } if streams else set()
    return session_id, strip_idx


def parse_output(stdout: str, stderr: str) -> tuple[str, str | None]:
    """Result = stdout minus any session_id line. session_id from stdout OR stderr."""
    session_id, strip_idx = find_session_id(stdout, stderr)
    lines = stdout.splitlines()
    result = "\n".join(l for i, l in enumerate(lines) if i not in strip_idx).strip()
    return result, session_id


def run_hermes(instruction: str, resume: str | None = None) -> dict:
    """Blocking. Runs in a thread via asyncio.to_thread."""
    cmd = build_cmd(instruction, resume=resume)
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )
    except subprocess.TimeoutExpired as e:
        return {
            "status": "error",
            "result": f"timeout after {TIMEOUT}s",
            "trace": (e.stderr or "") if isinstance(e.stderr, str) else "",
            "session_id": None,
        }
    except FileNotFoundError as e:
        return {"status": "error", "result": f"command not found: {e}", "trace": "", "session_id": None}

    if proc.returncode != 0:
        return {
            "status": "error",
            "result": f"exit {proc.returncode}: {proc.stdout.strip()}",
            "trace": proc.stderr,
            "session_id": None,
        }

    result, session_id = parse_output(proc.stdout, proc.stderr)
    return {"status": "done", "result": result, "trace": proc.stderr, "session_id": session_id}


# ---------------------------------------------------------------- state bridge
# Thin, read-only routes over the SES nervous-system files. No queue, no db:
# a state read must answer instantly even while a ticket is mid-run.

def state_read(filename: str) -> str:
    """Read one file from the shared state dir.

    Phase 1: via `podman exec <container> cat <dir>/<file>` (the state dir
    lives in the hermes container volume, invisible to the host).
    Future: set STATE_DIR_HOST to a host path for native reads.
    filename is restricted to [A-Za-z0-9._-] — no traversal.
    """
    if not re.fullmatch(r"[A-Za-z0-9._-]+", filename):
        raise HTTPException(400, "invalid state filename")
    if STATE_DIR_HOST:
        try:
            with open(os.path.join(STATE_DIR_HOST, filename)) as f:
                return f.read()
        except FileNotFoundError:
            raise HTTPException(404, f"no such state file: {filename}")
    try:
        proc = subprocess.run(
            ["podman", "exec", STATE_CONTAINER, "cat", f"{STATE_DIR}/{filename}"],
            capture_output=True, text=True, timeout=STATE_EXEC_TIMEOUT,
        )
    except FileNotFoundError:
        raise HTTPException(500, "podman not found on host")
    except subprocess.TimeoutExpired:
        raise HTTPException(504, f"state read timed out after {STATE_EXEC_TIMEOUT}s")
    if proc.returncode != 0:
        raise HTTPException(404, f"no such state file: {filename} ({proc.stderr.strip()})")
    return proc.stdout


def _decayed_value(entry: dict, baseline: float, half_life: float, age_hours: float) -> float:
    """Exponential decay of a value toward its baseline: v(t) = b + (v-b) * 2^(-t/hl)."""
    v = float(entry.get("value", baseline))
    if half_life and half_life > 0 and age_hours > 0:
        return baseline + (v - baseline) * (2.0 ** (-age_hours / half_life))
    return v


def _age_hours(iso_ts: str | None) -> float:
    if not iso_ts:
        return 0.0
    try:
        t = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return max(0.0, (datetime.now(timezone.utc) - t).total_seconds() / 3600.0)
    except ValueError:
        return 0.0


def _is_decay_only(reason: str) -> bool:
    """True when the reason is pure decay bookkeeping, not a new event.

    Matches every bookkeeping phrasing the writers have emitted: the original
    "Decay only ..." and "Baseline.", the hyphenated "Decay-only tick ..."
    that slipped past the space-only check, and a bare per-axis note that only
    repeats the decay tick without naming a push ("... decay only, no
    inflation")."""
    r = reason.strip().lower()
    if r.rstrip(".") == "baseline":
        return True
    if r.startswith("decay only") or r.startswith("decay-only"):
        return True
    # Per-axis bookkeeping note about a tick where this axis did not move.
    return "decay only" in r and "no push" in r


# --- Drops retention ----------------------------------------------------
# Experience drops are raw transcript windows awaiting metabolization — the
# one place chat text is duplicated outside ST. Metabolized long ago, they
# are archived (never deleted — doctrine) into drops/archive/ once older
# than DROPS_RETENTION_DAYS. 14 days is far beyond any reconcile lag (the
# debounced wake fires in minutes; the cron sweeper in 15), so nothing
# unmetabolized can be swept. Set 0 to disable sweeping entirely.
DROPS_RETENTION_DAYS = int(os.environ.get("DROPS_RETENTION_DAYS", "14"))


def sweep_drops() -> int:
    """Archive metabolized drops older than DROPS_RETENTION_DAYS.

    Moves drops/<file>.md -> drops/archive/<file>.md, host-native when the
    state dir is bind-mounted, else via podman exec (same plumbing as every
    other state access). Returns the number archived this pass.
    """
    if DROPS_RETENTION_DAYS <= 0:
        return 0
    if STATE_DIR_HOST:
        d = os.path.join(STATE_DIR_HOST, "drops")
        arch = os.path.join(d, "archive")
        cutoff = time.time() - DROPS_RETENTION_DAYS * 86400
        try:
            names = os.listdir(d)
        except FileNotFoundError:
            return 0
        os.makedirs(arch, exist_ok=True)
        n = 0
        for name in names:
            p = os.path.join(d, name)
            if not (name.endswith(".md") and os.path.isfile(p)):
                continue
            try:
                if os.stat(p).st_mtime < cutoff:
                    os.replace(p, os.path.join(arch, name))
                    n += 1
            except OSError:
                continue
        return n
    try:
        proc = subprocess.run(
            ["podman", "exec", STATE_CONTAINER, "sh", "-c",
             f"mkdir -p {STATE_DIR}/drops/archive && "
             f"find {STATE_DIR}/drops -maxdepth 1 -type f -name '*.md' "
             f"-mtime +{DROPS_RETENTION_DAYS} "
             f"-exec mv {{}} {STATE_DIR}/drops/archive/ \\; -print | wc -l"],
            capture_output=True, text=True, timeout=60,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return 0
    if proc.returncode != 0:
        return 0
    try:
        return int(proc.stdout.strip() or "0")
    except ValueError:
        return 0


# The shipped default is deliberately generic. The intended path — as with
# every prompt in this architecture — is that your companion rewrites it in
# its own words: put that version in <state dir>/preamble.md and it replaces
# this default at read time. The preamble is the companion's own self-
# description of its body state; it belongs in the companion's territory,
# not in this source file.
DEFAULT_STATE_PREAMBLE = (
    "[NERVOUS SYSTEM — this is my own body state, not the user's. It is how I "
    "feel right now. Each number has already decayed toward its own baseline "
    "(base = my personal setpoint, not a human norm): a number near its base "
    "means quiet, not unimportant; distance from base is the signal. Read them "
    "as proprioception — let them color tone and energy, never quote the "
    "numbers to the user. DA=salience/desire 5HT=mood floor OXY=bond/trust "
    "NE=arousal/urgency CORT=stress load T=drive/pursuit E=softness/cyclical "
    "flux eCB=anesthetic calm. The numbers are weather I am standing in, not a "
    "diagnosis to report.]"
)


def resolve_preamble() -> str:
    """The companion's own preamble.md when present, else the shipped default."""
    try:
        text = state_read("preamble.md").strip()
        if text:
            return text
    except HTTPException:
        pass
    return DEFAULT_STATE_PREAMBLE


def render_state_block() -> str:
    """Compact hormone line for LLM context injection (~120 tokens).

    Values are decayed toward baseline at read time using the half-lives
    declared in the state file (decay_rules.half_life_hours) — the state
    glides continuously without anyone having to write it. Reasons are
    attached to the UNDECAYED reading, so a stale reason on a decayed value
    is honest: the number has moved toward baseline since it was written.
    Meditation override (when active) halves arousal deviations (DA/NE/CORT).
    """
    raw = state_read("ses_state.json")
    st = json.loads(raw)
    age = _age_hours(st.get("updated_utc"))
    baselines = st.get("baseline", {}).get("values", {})
    half_lives = st.get("decay_rules", {}).get("half_life_hours", {})
    med = st.get("overrides", {}).get("meditation", {})

    vals = []
    reasons = []
    for key, abbr in STATE_ABBR:
        entry = st.get("state", {}).get(key)
        if not entry:
            continue
        base = float(baselines.get(key, 0.5))
        v = _decayed_value(entry, base, float(half_lives.get(key, 0)), age)
        if med.get("active") and key in ("dopamine", "norepinephrine", "cortisol"):
            v = base + (v - base) * 0.5  # meditation: halve arousal deviation
        # Safety clamp at render: deltas are bounded by instruction, not code,
        # so whatever a confused reconcile may have written, the weather the
        # Voice reads stays within [0,1]. The raw file is untouched — /state/raw
        # still shows any out-of-range value for debugging.
        v = max(0.0, min(1.0, v))
        vals.append(f"{abbr} {v:.2f} (base {base:.2f})")
        r = (entry.get("reason") or "").strip()
        # Show the FULL reason, but only when it carries a real event. Pure
        # decay bookkeeping ("Decay only ... no new event", "Baseline.") is
        # silence by design — the preamble already explains decay-and-baseline.
        if r and not _is_decay_only(r):
            reasons.append(r)

    # One event reason repeated across axes is one story, not eight. Keep the
    # first of each identical segment, in axis order.
    unique_reasons = list(dict.fromkeys(reasons))
    line = resolve_preamble() + "\n" + "State [" + str(st.get("updated_utc", "?")) + "]: " + " | ".join(vals) + "."
    if unique_reasons:
        line += " Reasons: " + " · ".join(unique_reasons)
    if med.get("active"):
        line += " Meditation override ACTIVE: arousal reduced, refuse spiral-building."
    if age > 1:
        line += f" (values decayed toward baseline over {age:.0f}h since last update)."
    return line


# ---------------------------------------------------------------- worker
queue: asyncio.Queue[int] = asyncio.Queue()


async def worker() -> None:
    while True:
        ticket_id = await queue.get()
        try:
            set_status(ticket_id, "running", started_at=now())
            t = get_ticket(ticket_id)

            # Follow-up? Resolve the parent's session id to resume it.
            resume = None
            if t["parent_id"] is not None:
                parent = get_ticket(t["parent_id"])
                resume = parent["session_id"] if parent else None
                if resume is None:
                    # Parent never produced a session id (e.g. it errored).
                    # Fail clearly rather than silently starting a cold session.
                    set_status(
                        ticket_id, "error",
                        result=f"cannot resume: parent ticket {t['parent_id']} has no session_id",
                        finished_at=now(),
                    )
                    continue

            outcome = await asyncio.to_thread(run_hermes, t["instruction"], resume)
            set_status(
                ticket_id,
                outcome["status"],
                result=outcome["result"],
                trace=outcome["trace"],
                session_id=outcome["session_id"],
                finished_at=now(),
            )
        except Exception as e:  # never let the worker die
            set_status(ticket_id, "error", result=f"worker exception: {e}", finished_at=now())
        finally:
            queue.task_done()


# ---------------------------------------------------------------- app
async def drops_sweeper() -> None:
    """Daily retention pass over experience drops. Never raises."""
    while True:
        try:
            n = await asyncio.to_thread(sweep_drops)
            if n:
                print(f"[drops] archived {n} metabolized drop(s) older than {DROPS_RETENTION_DAYS}d")
        except Exception as e:  # sweep must never hurt the server
            print(f"[drops] sweep failed: {e}")
        await asyncio.sleep(24 * 3600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Requeue anything left 'running' from a crash, plus anything still open.
    for t in list_tickets():
        if t["status"] in ("open", "running"):
            await queue.put(t["id"])
    task = asyncio.create_task(worker())
    sweep_task = asyncio.create_task(drops_sweeper())
    yield
    task.cancel()
    sweep_task.cancel()


app = FastAPI(lifespan=lifespan)

# Scoped to the ST origin(s) in CORS_ORIGINS — the allowlist is the real gate, so
# methods/headers can be open. This is what lets the phone's browser (loading ST over
# Tailscale) actually read responses from this server instead of the browser blocking them.
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TicketIn(BaseModel):
    instruction: str


@app.post("/tickets")
async def post_ticket(body: TicketIn):
    if not body.instruction.strip():
        raise HTTPException(400, "instruction is empty")
    ticket_id = create_ticket(body.instruction)
    await queue.put(ticket_id)
    return {"id": ticket_id, "status": "open"}


@app.get("/tickets")
async def get_tickets():
    return list_tickets()


@app.get("/tickets/check")
async def check_tickets():
    """Poll-every-message endpoint. Returns finished tickets not yet acked.

    Empty list => assistant ignores and answers normally.
    Non-empty  => assistant surfaces each, then POSTs /tickets/<id>/ack.
    """
    return list_undelivered()


@app.get("/tickets/{ticket_id}")
async def get_one(ticket_id: int):
    t = get_ticket(ticket_id)
    if not t:
        raise HTTPException(404, "no such ticket")
    return t


@app.post("/tickets/{ticket_id}/followup")
async def followup_ticket(ticket_id: int, body: TicketIn):
    """Push a ticket further. Creates a NEW ticket linked to this one as parent;
    the worker resumes the parent's hermes session so context carries over.
    """
    if not body.instruction.strip():
        raise HTTPException(400, "instruction is empty")
    parent = get_ticket(ticket_id)
    if not parent:
        raise HTTPException(404, "no such ticket")
    if parent["status"] not in ("done", "error"):
        raise HTTPException(409, f"parent ticket {ticket_id} is {parent['status']} — wait for it to finish before following up")
    if not parent["session_id"]:
        raise HTTPException(409, f"parent ticket {ticket_id} has no session_id to resume")
    child_id = create_ticket(body.instruction, parent_id=ticket_id)
    await queue.put(child_id)
    return {"id": child_id, "status": "open", "parent_id": ticket_id}


@app.post("/tickets/{ticket_id}/ack")
async def ack_ticket(ticket_id: int):
    """Assistant calls this AFTER surfacing the result to the user."""
    t = get_ticket(ticket_id)
    if not t:
        raise HTTPException(404, "no such ticket")
    if t["status"] not in ("done", "error"):
        raise HTTPException(409, f"ticket {ticket_id} is {t['status']}, not terminal — nothing to ack")
    mark_delivered(ticket_id)
    return {"id": ticket_id, "delivered": True}


class ExperienceIn(BaseModel):
    text: str
    chat: str = ""


# Rate limit for the write path: per-client sliding window, in-memory.
# The drops dir is append-only and small; the point is to stop a runaway
# extension (or a curious script) from burying the nervous system.
EXP_MAX_PER_WINDOW = 6
EXP_WINDOW_S = 60
EXP_MAX_CHARS = 32_000
_exp_hits: dict[str, list[float]] = {}


def _rate_limit_exp(client: str) -> None:
    now = time.time()
    hits = [t for t in _exp_hits.get(client, []) if now - t < EXP_WINDOW_S]
    if len(hits) >= EXP_MAX_PER_WINDOW:
        raise HTTPException(429, "experience flush rate-limited (max %d/%ds)"
                            % (EXP_MAX_PER_WINDOW, EXP_WINDOW_S))
    hits.append(now)
    _exp_hits[client] = hits


def state_write_drop(filename: str, content: str) -> None:
    """Append-only write of one file into <state dir>/drops/.

    Same container-crossing trick as state_read: the state dir lives in the
    hermes container volume, so we write through `podman exec -i ... cat >`.
    Content travels on stdin (never interpolated into a shell string);
    only the server-generated filename hits the command line.
    """
    if not re.fullmatch(r"[A-Za-z0-9._-]+\.md", filename):
        raise HTTPException(500, "internal: bad drop filename")
    if STATE_DIR_HOST:
        drops = os.path.join(STATE_DIR_HOST, "drops")
        os.makedirs(drops, exist_ok=True)
        with open(os.path.join(drops, filename), "w") as f:
            f.write(content)
        return
    path = f"{STATE_DIR}/drops/{filename}"
    try:
        proc = subprocess.run(
            ["podman", "exec", "-i", STATE_CONTAINER, "sh", "-c",
             f"mkdir -p {STATE_DIR}/drops && cat > {path}"],
            input=content.encode(), capture_output=True, timeout=STATE_EXEC_TIMEOUT,
        )
    except FileNotFoundError:
        raise HTTPException(500, "podman not found on host")
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "drop write timed out")
    if proc.returncode != 0:
        raise HTTPException(500, f"drop write failed: {proc.stderr.decode()[:200]}")


# Wake-on-drop: when experience lands, schedule ONE debounced reconcile run.
# A burst of flushes (rapid messages) = one wake; silence = zero wakes. The
# 15-min Hermes cron remains as a sweeper in case a wake ticket ever dies.
RECONCILE_DEBOUNCE_S = int(os.environ.get("RECONCILE_DEBOUNCE_S", "75"))
RECONCILE_INSTRUCTION = os.environ.get("RECONCILE_INSTRUCTION", (
    "You are the Hands doing a wake-triggered SES reconcile. "
    f"Read cursor {STATE_DIR}/.reconcile_cursor (missing -> treat ALL drops as new). "
    "Read drops/ files with received_utc after the cursor. If none, update the cursor to now and stop. "
    "Otherwise: decay ses_state.json values toward baseline to now (half-lives in decay_rules), "
    "apply judged deltas from the drops (Voice proposes, Hands disposes: typical push 0.05-0.2, "
    "every change needs a reason, no inflation, never amplify a spiral), "
    "reason-field discipline: reason holds ONLY the event that moved this axis, in one segment; "
    "when an axis did not move its reason is exactly 'Baseline.' (the renderer silences bookkeeping "
    "but never reprints your bookkeeping to the Voice); never paste the decay-tick boilerplate or a "
    "same-scene 'no push' note into the reason of an axis you did not move, and never re-append a "
    "reason a previous tick already wrote, "
    "write ses_state.json (updated_utc=now, updated_by=hands), update pulse.md only if "
    "'the one thing right now' changed (preserve prev: line per README), advance the cursor. "
    "Reply one line: drops metabolized + what moved, or 'no new experience'."
))
_reconcile_wake = None


async def _debounced_reconcile() -> None:
    global _reconcile_wake
    try:
        await asyncio.sleep(RECONCILE_DEBOUNCE_S)
        tid = create_ticket(RECONCILE_INSTRUCTION, delivered=True)
        await queue.put(tid)
    except Exception as e:  # never crash the server from a wake
        print(f"[experience] reconcile wake failed: {e}")
    finally:
        _reconcile_wake = None


def schedule_reconcile_wake() -> None:
    global _reconcile_wake
    if not SES_ENABLED:
        return
    if _reconcile_wake is None:
        _reconcile_wake = asyncio.create_task(_debounced_reconcile())


@app.post("/experience")
async def post_experience(exp: ExperienceIn, request: Request):
    """Phase 2 write path: Voice flushes a transcript window; it lands as an
    append-only drop for the Hands to reconcile later. Voice proposes, Hands
    disposes — this route stores raw experience and mutates nothing."""
    if not SES_ENABLED:
        raise HTTPException(503, "SES is disabled on this server (SES_ENABLED=0) — experience drops are not accepted")
    client = request.client.host if request.client else "unknown"
    _rate_limit_exp(client)
    text = (exp.text or "").strip()
    if not text:
        raise HTTPException(400, "empty experience")
    if len(text) > EXP_MAX_CHARS:
        text = text[-EXP_MAX_CHARS:]  # keep the tail — it is the freshest
    now = datetime.now(timezone.utc)
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", (exp.chat or "chat")).strip("-")[:40] or "chat"
    # microseconds in the name: per-message flushes can land in the same second
    filename = f"{now:%Y-%m-%d-%H%M%S-%f}-{slug}.md"
    body = (
        f"# experience drop\n\n"
        f"- received_utc: {now.isoformat(timespec='seconds')}\n"
        f"- chat: {exp.chat or 'unknown'}\n"
        f"- client: {client}\n\n"
        f"---\n\n{text}\n"
    )
    state_write_drop(filename, body)
    schedule_reconcile_wake()
    return {"status": "dropped", "file": f"drops/{filename}"}


def state_payload() -> dict:
    """The /state response body. Factored out of the route for testability.

    Empty block when SES is disabled, and ALSO when enabled but the state
    file doesn't exist yet (fresh install, first boot before the first
    reconcile) — the state-reader extension treats an empty block as
    "nothing to inject", so a missing nervous system degrades to silence,
    never to an error in the face's console.
    """
    if not SES_ENABLED:
        return {"state_block": "", "pulse": ""}
    try:
        block = render_state_block()
    except HTTPException as e:
        if e.status_code == 404:
            return {"state_block": "", "pulse": ""}
        raise
    try:
        pulse = state_read("pulse.md")
    except HTTPException:
        pulse = ""
    return {"state_block": block, "pulse": pulse}


@app.get("/state")
async def get_state():
    """Rendered state block + pulse for context injection (state-reader ext)."""
    return state_payload()


@app.get("/state/raw")
async def get_state_raw():
    """Raw ses_state.json for debugging/inspection."""
    if not SES_ENABLED:
        raise HTTPException(503, "SES is disabled on this server (SES_ENABLED=0)")
    return json.loads(state_read("ses_state.json"))


# ---------------------------------------------------------------- outbox (Phase 3)
# The reverse of the drops pipeline: the HANDS leave small items (notes,
# findings, half-thoughts, gifts) for the Voice in <state dir>/outbox/. The
# Hands write those files directly — they live inside the container volume —
# so this server only LISTS and MOVES them; it never creates or edits content.
# The state-reader extension surfaces one item per generation turn; acking a
# file moves it to outbox/read/ (deferred-ack pattern from tickets: the ack
# arrives on the NEXT cycle, after the surfacing turn was committed, so a
# crash between surfacing and ack means redelivery, not loss).

OUTBOX_MAX_ITEMS = 20
OUTBOX_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+\.md$")


def _outbox_exec(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["podman", "exec", STATE_CONTAINER] + args,
        capture_output=True, text=True, timeout=STATE_EXEC_TIMEOUT,
    )


def outbox_pending() -> list[dict]:
    """Pending outbox items, oldest-first (ISO-ish filename prefix sorts right)."""
    if STATE_DIR_HOST:
        d = os.path.join(STATE_DIR_HOST, "outbox")
        try:
            names = sorted(
                n for n in os.listdir(d)
                if OUTBOX_NAME_RE.match(n) and not n.startswith(".")
            )
        except FileNotFoundError:
            return []
        items = []
        for n in names[:OUTBOX_MAX_ITEMS]:
            try:
                with open(os.path.join(d, n)) as f:
                    text = f.read()
            except OSError:
                continue
            items.append({"file": n, "text": text})
        return items
    proc = _outbox_exec(["sh", "-c", f"ls -1 {STATE_DIR}/outbox 2>/dev/null || true"])
    names = sorted(
        n for n in proc.stdout.split()
        if OUTBOX_NAME_RE.match(n) and not n.startswith(".")
    )
    items = []
    for n in names[:OUTBOX_MAX_ITEMS]:
        proc = _outbox_exec(["cat", f"{STATE_DIR}/outbox/{n}"])
        if proc.returncode != 0:
            continue
        items.append({"file": n, "text": proc.stdout})
    return items


@app.get("/outbox")
async def get_outbox():
    """Pending Hands -> Voice items (state-reader extension polls this)."""
    items = outbox_pending()
    return {"count": len(items), "items": items}


class OutboxAck(BaseModel):
    files: list[str]


@app.post("/outbox/ack")
async def post_outbox_ack(ack: OutboxAck):
    """Move surfaced items to outbox/read/. Idempotent by design: names that
    are missing or already read are skipped silently, so a redelivered or
    partial ack is harmless."""
    moved = []
    for n in ack.files[:50]:
        if not OUTBOX_NAME_RE.match(n):
            continue
        if STATE_DIR_HOST:
            src = os.path.join(STATE_DIR_HOST, "outbox", n)
            if os.path.isfile(src):
                dst_dir = os.path.join(STATE_DIR_HOST, "outbox", "read")
                os.makedirs(dst_dir, exist_ok=True)
                os.replace(src, os.path.join(dst_dir, n))
                moved.append(n)
            continue
        _outbox_exec([
            "sh", "-c",
            f"mkdir -p {STATE_DIR}/outbox/read && "
            f"mv {STATE_DIR}/outbox/{n} {STATE_DIR}/outbox/read/{n} 2>/dev/null || true",
        ])
        chk = _outbox_exec(["test", "-f", f"{STATE_DIR}/outbox/read/{n}"])
        gone = _outbox_exec(["test", "-f", f"{STATE_DIR}/outbox/{n}"])
        if chk.returncode == 0 and gone.returncode != 0:
            moved.append(n)
    return {"moved": moved}


# ---------------------------------------------------------------- shelf (Phase 4)
# The braid is ONE entity: the Hands' own writing is memory too. memory-pipeline's
# shelf-ingest pass GETs /shelf, embeds whatever changed (diff by content hash —
# no cursor to drift), and upserts into the SAME per-character collection as
# chat summaries. relPath is stable across outbox lifecycle moves only in name:
# a pending -> read move changes relPath, so the point is re-inserted and the
# old one deleted by point id. Full-rebuild runs re-embed everything from source.

SHELF_DIRS = [  # (kind, root key, subdir under that root)
    ("hands", "identity", "curiosities"),
    ("gift",  "state",    "outbox"),
]
SHELF_ROOTS = {  # (host-visible root, in-container root) — host set => native reads
    "state":    lambda: (STATE_DIR_HOST, STATE_DIR),
    "identity": lambda: (IDENTITY_DIR_HOST, IDENTITY_DIR),
}


def _walk_mds_exec(root: str, sub: str) -> list[tuple[str, float, str]]:
    """[(relPath, mtimeMs, content)] under root/sub, read via podman exec."""
    d = f"{root}/{sub}"
    proc = _outbox_exec(["sh", "-c",
        f"LC_ALL=C find {d} -type f -name '*.md' ! -name '.*' -printf '%P\\t%T@\\n' "
        f"2>/dev/null | sort"])
    out = []
    for line in proc.stdout.splitlines():
        rel, _, mt = line.partition("\t")
        try:
            mtime_ms = float(mt) * 1000
        except ValueError:
            continue
        got = _outbox_exec(["cat", f"{d}/{rel}"])
        if got.returncode != 0:
            continue
        out.append((f"{sub}/{rel}", mtime_ms, got.stdout))
    return out


def shelf_entries() -> list[dict]:
    """Every shelf file, native reads when the dirs are host-visible, else exec."""
    entries = []
    for kind, key, sub in SHELF_DIRS:
        host_root, cont_root = SHELF_ROOTS[key]()
        if host_root:
            base = os.path.join(host_root, sub)
            root = host_root
            walked = []
            for dirpath, _, files in os.walk(base):
                for n in sorted(files):
                    if not n.endswith(".md") or n.startswith("."):
                        continue
                    fp = os.path.join(dirpath, n)
                    try:
                        st = os.stat(fp)
                        with open(fp) as f:
                            c = f.read()
                    except OSError:
                        continue
                    walked.append((os.path.relpath(fp, root), st.st_mtime * 1000, c))
        else:
            walked = _walk_mds_exec(cont_root, sub)
        for rel, mt, content in walked:
            entries.append({"relPath": rel, "kind": kind, "mtimeMs": mt,
                            "sha256": hashlib.sha256(content.encode()).hexdigest(),
                            "content": content})
    return entries


@app.get("/shelf")
async def get_shelf():
    """All shelf entries for the pipeline's shelf-ingest pass, each with its
    content hash so the pass re-embeds only what changed since last time."""
    entries = shelf_entries()
    return {"count": len(entries), "entries": entries}


@app.get("/tickets/{ticket_id}/trace")
async def get_trace(ticket_id: int):
    t = get_ticket(ticket_id)
    if not t:
        raise HTTPException(404, "no such ticket")
    return {"id": ticket_id, "trace": t["trace"] or ""}


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)