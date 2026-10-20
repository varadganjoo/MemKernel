"""Two independent local databases exchanging notices through an explicit JSON file.

Run: python examples/portable_notices.py
No server, network, model calls, credentials or permanent files are needed.
"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from memkernel import MemKernel


async def main() -> None:
    with TemporaryDirectory(prefix="memkernel-notices-") as directory:
        root = Path(directory)
        async with await MemKernel.open(root / "alice.db", tokenizer="heuristic") as alice, await MemKernel.open(
            root / "bob.db", tokenizer="heuristic"
        ) as bob:
            notice = await alice.notices.post(
                agent_id="alice", resource="service:auth",
                message="Changing login. Please coordinate with me before touching auth.",
            )
            assert await bob.notices.list() == []
            exchange = root / "notices.json"
            exchange.write_text(await alice.notices.export_json(notice_ids=[notice.notice_id]), encoding="utf-8")
            old_export = exchange.read_text(encoding="utf-8")
            assert await bob.notices.import_json(old_export) == 1
            assert await bob.notices.import_json(old_export) == 0
            print("PASS: explicit file exchange reaches a separate database; duplicates are harmless")

            await bob.fork(source_branch="main", new_branch="investigate-login", agent_id="bob")
            state = await bob.query_state("investigate-login")
            assert state["coordination_notices"][0]["notice_id"] == notice.notice_id
            print("PASS: the notice stays visible on an investigation branch")

            bob.notices.clock = lambda: notice.expires_at
            assert (await bob.notices.list())[0]["status"] == "stale"
            print("PASS: expiry means stale, not permission to assume work is finished")

            await alice.notices.clear(notice.notice_id, agent_id="alice")
            exchange.write_text(await alice.notices.export_json(), encoding="utf-8")
            assert await bob.notices.import_json(exchange.read_text(encoding="utf-8")) == 1
            assert await bob.notices.import_json(old_export) == 0
            assert await bob.notices.list() == []
            print("PASS: clearing propagates; a delayed active export cannot reopen the notice")


if __name__ == "__main__":
    asyncio.run(main())
