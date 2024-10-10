"""Module containing protocols that prescribe disagain implementations."""

import collections.abc
import typing

if typing.TYPE_CHECKING:
    import typing_extensions

__all__: collections.abc.Sequence[str] = ("CommandProto", "ConnectionProto")


class CommandProto(typing.Protocol):
    """Redis command protocol."""

    def arg(self, value: str | bytes | int | float) -> "CommandProto":
        """Add an argument to this command."""
        ...

    def __iter__(self) -> typing.Iterator[bytes]: ...

    def __len__(self) -> int: ...


class ConnectionProto(typing.Protocol):
    """Redis connection protocol."""

    @classmethod
    async def from_host_port(cls, host: str, port: int, /) -> "typing_extensions.Self":
        """Connect to Redis at the provided host and port."""
        ...

    async def connect(self) -> None:
        """Connect to Redis with the connection parameters provided at instantiation."""
        ...

    async def disconnect(self) -> None:
        """Close the connection with Redis."""
        ...

    async def write_command(self, command: "CommandProto", /) -> None:
        """Write a command to the connected Redis instance.

        This requires this connection to be alive.

        Either ``read_response`` or ``discard_response`` *must* be called after
        this.
        """
        ...

    async def read_response(self, *, disconnect_on_error: bool) -> typing.Any:  # noqa: ANN401
        """Read the response to a previously executed command.

        This requires this connection to be alive.
        """
        ...

    async def discard_response(self, *, disconnect_on_error: bool) -> typing.Any:  # noqa: ANN401
        """Discard the response to the previously executed command.

        This requires this connection to be alive.
        """
        ...
