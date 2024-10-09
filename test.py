import asyncio
import disagain


async def main():
    con = await disagain.ActionableConnection.from_url("redis://127.0.0.1:6379")
    print("connected")

    # XREAD COUNT 1 BLOCK 10000 STREAMS message_interaction:0 0
    r1 = await con.xread({"message_interaction:0": 0}, count=1, block=10000)
    print(r1)

    # or (i wish python supported this without parentheses)
    cmd = (
        disagain.Command("XREAD")
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(10000)
            .arg("STREAMS")
            .arg("message_interaction:0")
            .arg(0)
    )
    r2 = await cmd.execute(con)
    print(r2)

    assert r1 == r2

    await con.disconnect()

    await asyncio.sleep(1)


asyncio.run(main())
