import collections.abc
import dataclasses
import typing

from disagain import protocol

if typing.TYPE_CHECKING:
    import typing_extensions

__all__: collections.abc.Sequence[str] = ("Command",)


@dataclasses.dataclass(slots=True)
class Command:
    arguments: list[bytes]
    discard_response: bool
    disconnect_on_error: bool

    def __init__(self, name: str | bytes, *args: str | bytes | int | float):
        self.discard_response = False
        self.disconnect_on_error = True

        self.arguments = []
        self.arg(name)
        for arg in args:
            self.arg(arg)

    def arg(self, value: str | bytes | int | float) -> "typing_extensions.Self":
        if isinstance(value, bytes):
            pass
        elif isinstance(value, str):
            value = value.encode()
        elif isinstance(value, (int, float)):
            value = str(value).encode()

        self.arguments.append(value)
        return self

    def set_discard_response(self, discard_response: bool) -> "typing_extensions.Self":
        self.discard_response = discard_response
        return self

    def set_disconnect_on_error(self, disconnect_on_error: bool) -> "typing_extensions.Self":
        self.disconnect_on_error = disconnect_on_error
        return self

    async def execute(self, con: protocol.ConnectionProto) -> typing.Any:
        await con.write_command(self)

        if self.discard_response:
            return await con.discard_response(disconnect_on_error=self.disconnect_on_error)

        return await con.read_response(disconnect_on_error=self.disconnect_on_error)

    def __str__(self):
        return "".join(arg.decode("utf-8", errors="replace") for arg in self.arguments)
    
    def __len__(self) -> int:
        return len(self.arguments)
    
    def __iter__(self) -> collections.abc.Iterator[bytes]:
        return iter(self.arguments)
