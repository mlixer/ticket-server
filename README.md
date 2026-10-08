# ticket-server

The agency bus of [The Braid](https://github.com/mlixer/the-braid): a small
FastAPI service that runs an agent CLI (hermes) headless, one ticket at a
time, and bridges its work — and its synthetic endocrine state — into
SillyTavern.

**What it does**

- **Tickets**: accepts instruction strings, runs them through `hermes chat`
  via `podman exec` as resumable sessions, queues one at a time, serves
  status/result/trace over HTTP. Ticket runs are locked down per invocation:
  a narrow toolset allowlist and a turn cap, re-applied even on resumes.
- **SES (optional)**: the nervous system. `/state` renders a compact hormone
  block with read-time decay toward per-axis baselines; `/experience` accepts
  transcript drops from the face and schedules one debounced reconcile wake;
  `/outbox` carries the hands' gifts back, with deferred ack; `/shelf` feeds
  the hands' own writing into memory. Set `SES_ENABLED=0` for a tickets-only
  server — everything degrades gracefully.
- **Loopback forever**: binds 127.0.0.1. For phone access, front it with a
  TLS reverse proxy on your private network (e.g. `tailscale serve` on
  :8443) and add your ST origin to `TICKET_CORS_ORIGINS`.

**Install**

```
python3 -m venv .venv && .venv/bin/pip install fastapi uvicorn pydantic
cp ticket-server.service ~/.config/systemd/user/   # edit the <<< EDIT lines first
systemctl --user daemon-reload && systemctl --user enable --now ticket-server
```

Pairs with the ST extensions
[st-ticket-tools](https://github.com/mlixer/st-ticket-tools) (file/track/
deliver tickets in chat) and
[st-state-bridge](https://github.com/mlixer/st-state-bridge) (state
injection, experience flush, gifts).

Companion-authored preamble: put your companion's own rendition of the state
preamble in `<state dir>/preamble.md` and the server prefers it over the
shipped default — the voice of its body belongs to it.

## License

AGPL-3.0.
