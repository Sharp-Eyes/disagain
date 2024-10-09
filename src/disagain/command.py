import collections.abc
import dataclasses
import typing

from disagain import protocol

__all__: collections.abc.Sequence[str] = ("Command",)


@dataclasses.dataclass(slots=True)
class Command:
    arguments: list[bytes]

    def __init__(self, name: str | bytes, *args: str | bytes | int | float):
        self.arguments = []
        self.arg(name)
        for arg in args:
            self.arg(arg)

    def arg(self, value: str | bytes | int | float) -> "Command":
        if isinstance(value, bytes):
            pass
        elif isinstance(value, str):
            value = value.encode()
        elif isinstance(value, (int, float)):
            value = str(value).encode()

        self.arguments.append(value)
        return self

    async def execute(self, con: protocol.ConnectionProto) -> typing.Any:
        await con.write_command(self)
        return await con.read_response(disconnect_on_error=True) 

    def __str__(self):
        return "".join(arg.decode("utf-8", errors="replace") for arg in self.arguments)
    
    def __len__(self) -> int:
        return len(self.arguments)
    
    def __iter__(self) -> collections.abc.Iterator[bytes]:
        return iter(self.arguments)
