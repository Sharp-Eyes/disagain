import collections.abc
import typing

if typing.TYPE_CHECKING:
    import typing_extensions

__all__: collections.abc.Sequence[str] = ("CommandProto", "ConnectionProto")


class CommandProto(typing.Protocol):
    def arg(self, value: str | bytes | int | float) -> "CommandProto": ...

    def __iter__(self) -> typing.Iterator[bytes]: ...

    def __len__(self) -> int: ...


class ConnectionProto(typing.Protocol):
    @classmethod
    async def from_host_port(cls, host: str, port: int, /) -> "typing_extensions.Self": ...

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def write_command(self, command: "CommandProto", /) -> None: ...

    async def read_response(self, *, disconnect_on_error: bool) -> typing.Any: ...

    async def discard_response(self, *, disconnect_on_error: bool) -> typing.Any: ...
