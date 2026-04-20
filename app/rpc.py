from __future__ import annotations

import dataclasses
from typing import Any, Final

import orjson
from sslog import logger

from app.db import Database
from app.utils import parse_obj

# RPC method names
RPC_DELETE_TORRENT: Final = "delete-torrent"
RPC_PING: Final = "ping"

ALLOWED_METHODS: frozenset[str] = frozenset({RPC_DELETE_TORRENT, RPC_PING})


# --- Payload dataclasses ---


@dataclasses.dataclass(frozen=True, kw_only=True)
class DeleteTorrentPayload:
    info_hash: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class PingPayload:
    pass


# Method name → payload class mapping
PAYLOAD_TYPES: dict[str, type[Any]] = {
    RPC_DELETE_TORRENT: DeleteTorrentPayload,
    RPC_PING: PingPayload,
}


# Type alias: handler receives a typed payload, returns any serializable result
type RpcHandler = Any


def process_commands(
    db: Database,
    node_id: str,
    handlers: dict[str, RpcHandler],
) -> None:
    """Poll and execute pending RPC commands for this node."""
    rows: list[tuple[int, str, str]] = db.fetch_all(
        """select id, method, payload from node_command
           where node_id = $1 and executed_at is null
           order by id""",
        [node_id],
    )
    if not rows:
        return

    for cmd_id, method, payload_str in rows:
        result: str | None = None
        error: str | None = None

        handler = handlers.get(method)
        payload_cls = PAYLOAD_TYPES.get(method)
        if handler is None or payload_cls is None:
            continue

        try:
            raw = orjson.loads(payload_str)
            payload = parse_obj(payload_cls, raw)
            ret = handler(payload)
            result = orjson.dumps(ret).decode()
        except Exception as e:
            error = str(e)
        db.execute(
            """update node_command
               set executed_at = current_timestamp, result = $1, error = $2
               where id = $3""",
            [result, error, cmd_id],
        )
        logger.info("rpc command {} method={} error={}", cmd_id, method, error)


@dataclasses.dataclass(frozen=True, kw_only=True)
class RpcRequest:
    method: str
    payload: dict[str, Any] = dataclasses.field(default_factory=dict)


async def enqueue_command(
    pool: Any,
    node_id: str,
    method: str,
    payload: Any,
) -> int:
    """Insert a new RPC command into the queue and return its id."""
    cmd_id: int = await pool.fetchval(
        """insert into node_command (node_id, method, payload)
           values ($1, $2, $3) returning id""",
        node_id,
        method,
        orjson.dumps(payload).decode(),
    )
    await pool.execute("SELECT pg_notify($1, $2)", f"node_rpc_{node_id}", node_id)
    return cmd_id
