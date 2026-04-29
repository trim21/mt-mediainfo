---
name: rpc-system
description: "RPC command queue system for server-to-downloader communication. Use when working on RPC methods, command dispatch, or the /rpc and /api/node/{node_id}/rpc endpoints in app/rpc.py, app/downloader.py, app/server.py."
user-invocable: false
---

# RPC System

The RPC system enables the web server to dispatch commands to specific downloader nodes asynchronously via a PostgreSQL-backed command queue (`node_command` table).

## Architecture

1. **Server** enqueues commands via `POST /api/node/{node_id}/rpc` → inserts into `node_command` table
2. **Downloader** polls `node_command` for pending commands each iteration (before processing torrents)
3. **Downloader** executes the handler, writes `result`/`error` and `executed_at` back to the row
4. **Web UI** at `/rpc` shows command history with status (pending/done/error)

## Database Table

```sql
create table if not exists node_command (
    id bigserial primary key,
    node_id uuid not null,
    method text not null,
    payload text not null default '{}',
    result text,
    error text,
    created_at timestamptz not null default now(),
    executed_at timestamptz
);

create index if not exists idx_node_command_pending
    on node_command (node_id) where executed_at is null;
```

## RPC Methods

Defined in `app/rpc.py`:

| Method           | Payload                | Handler                                  | Description                                                                                         |
| ---------------- | ---------------------- | ---------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `delete-torrent` | `{"info_hash": "..."}` | `Downloader.__handle_cmd_delete_torrent` | Deletes torrent from qBittorrent (with files) and marks job as failed with reason "deleted by user" |
| `ping`           | `{}`                   | `Downloader.__handle_cmd_ping`           | Returns `{"pong": "ok"}`, used for connectivity testing                                             |

## Key Types (`app/rpc.py`)

- `RPC_DELETE_TORRENT: Final = "delete-torrent"` / `RPC_PING: Final = "ping"` — Method name constants
- `ALLOWED_METHODS: frozenset[str]` — Whitelist of valid RPC method names; server rejects unknown methods with 400
- `DeleteTorrentPayload` / `PingPayload` — Frozen dataclasses for typed payload deserialization
- `PAYLOAD_TYPES: dict[str, type]` — Maps method name → payload class
- `process_commands(db, node_id, handlers)` — Polls and executes pending commands (sync, called by downloader)
- `enqueue_command(pool, node_id, method, payload)` — Inserts a new command (async, called by server)

## Downloader-Side Handler Registration (`app/downloader.py`)

```python
def __process_commands(self) -> None:
    process_commands(
        self.db,
        self.config.node_id,
        {
            RPC_DELETE_TORRENT: self.__handle_cmd_delete_torrent,
            RPC_PING: self.__handle_cmd_ping,
        },
    )
```

## Server-Side Dispatch (`app/server.py`)

```
POST /api/node/{node_id}/rpc
Body: {"method": "delete-torrent", "payload": {"info_hash": "abc123..."}}
Response: {"id": 42}
```

The server validates the node exists and the method is in `ALLOWED_METHODS` before enqueuing.

## Adding a New RPC Method

1. Define a new payload dataclass in `app/rpc.py`:
   ```python
   @dataclasses.dataclass(frozen=True, kw_only=True)
   class MyPayload:
       some_field: str
   ```
2. Add method name constant: `RPC_MY_METHOD: Final = "my-method"`
3. Add to `ALLOWED_METHODS` frozenset and `PAYLOAD_TYPES` dict
4. Implement handler in `app/downloader.py` `Downloader` class:
   ```python
   def __handle_cmd_my_method(self, payload: MyPayload) -> dict[str, str]:
       # ... do work ...
       return {"status": "ok"}
   ```
5. Register in `Downloader.__process_commands()` handlers dict

## HTTP Surfaces

- `POST /api/node/{node_id}/rpc` validates the target node and method, then enqueues the command
- `GET /rpc` shows recent command history with derived `pending`, `done`, or `error` status
- The broader dashboard and admin routes live in the `server-dashboard` skill; keep this skill focused on the RPC queue itself

## Related Files

- `app/rpc.py` — RPC framework (methods, payloads, process/enqueue)
- `app/downloader.py` — Handler implementations, command polling in main loop
- `app/server.py` — HTTP endpoint for dispatching commands
- `app/templates/rpc.html.j2` — RPC history page template
- `app/sql/schema.sql` — `node_command` table definition
- `server-dashboard` skill — broader FastAPI route and template behavior
