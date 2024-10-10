"""Module containing Redis client implementation."""

import asyncio
import collections.abc
import dataclasses
import types
import typing
import urllib.parse

from disagain import connection, protocol

if typing.TYPE_CHECKING:
    import typing_extensions

__all__: collections.abc.Sequence[str] = ("Redis",)


ConnectionT = typing.TypeVar("ConnectionT", bound=protocol.ConnectionProto)


@dataclasses.dataclass(slots=True)
class Redis:
    """Redis client implementation."""

    host: str
    port: int

    _connections: list[protocol.ConnectionProto] = dataclasses.field(
        default_factory=list,
        init=False,
    )

    @classmethod
    def from_url(cls, url: str) -> "Redis":
        """Create a Redis client from a Redis url.

        This performs URL validation, but does *not* make any connections.

        Connections should be created by the user with ``get_connection``.
        """
        parsed = urllib.parse.urlparse(url)
        if not parsed.hostname or not parsed.port or parsed.scheme != "redis":
            msg = "Only urls of scheme 'redis://host:port' are supported"
            raise ValueError(msg)

        return cls(parsed.hostname, parsed.port)

    async def get_connection(
        self,
        connection_class: type[ConnectionT] = connection.ActionableConnection,
    ) -> ConnectionT:
        """Make a new connection to this client's Redis instance.

        By default, this make a new ActionableConnection. You can provide a
        different (custom) connection class through the ``connection_class``
        argument.
        """
        connection = await connection_class.from_host_port(self.host, self.port)
        self._connections.append(connection)
        return connection

    async def disconnect(self) -> None:
        """Disconnect all connections registered to this Redis client."""
        await asyncio.gather(*[connection.disconnect() for connection in self._connections])

    async def __aenter__(self) -> "typing_extensions.Self":
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _exc_tb: types.TracebackType | None,
    ) -> None:
        await self.disconnect()
