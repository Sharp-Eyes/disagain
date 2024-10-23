"""Test module."""

import asyncio

import disagain


async def _main() -> None:
    async with disagain.Redis.from_url("redis://127.0.0.1:6379") as client:
        con = await client.get_connection()

        # XREAD COUNT 1 BLOCK 10000 STREAMS message_interaction:0 0
        r1 = await con.hset("foo", foo="bar", field="value")
        print(r1, type(r1))

        r2 = await con.hget("foo", "foo")
        print(r2, type(r2))

        r3 = await con.hgetall("foo")
        print(r3)

        r4 = await con.hdel("foo", "field")
        print(r4)

        r5 = await con.hgetall("foo")
        print(r5)


asyncio.run(_main())
