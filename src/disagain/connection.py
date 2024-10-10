"""Module containing connection implementations."""

import asyncio
import collections.abc
import dataclasses
import enum
import socket
import typing
import urllib.parse
import weakref

from disagain import command, error, protocol

if typing.TYPE_CHECKING:
    import typing_extensions

__all__: collections.abc.Sequence[str] = ("Connection", "ActionableConnection")


_RESP3: typing.Final = 3

ConnectHook: typing.TypeAlias = typing.Callable[
    ["Connection"],
    typing.Coroutine[typing.Any, typing.Any, None],
]


class ByteResponse(bytes, enum.Enum):
    # Ordered by documentation:
    # https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md

    # Simple types
    BLOB_STRING = b"$"
    SIMPLE_STRING = b"+"
    SIMPLE_ERROR = b"-"
    NUMBER = b":"
    NULL = b"_"
    DOUBLE = b","
    BOOLEAN = b"#"
    BLOB_ERROR = b"!"
    VERBATIM_STRING = b"="
    BIG_NUMBER = b"("

    # Aggregate types
    ARRAY = b"*"
    MAP = b"%"
    SET = b"~"
    ATTRIBUTE = b"|"
    PUSH = b">"


@dataclasses.dataclass(slots=True)
class Connection:
    """Low-level connection implementation.

    This connection can make connections to Redis, and both send and receive
    commands. It does not implement any higher-level commands.

    Only RESP3 connections are supported.
    """

    host: str
    port: int
    buffer_limit: int = 6000
    _post_connect_hooks: collections.abc.MutableMapping[str, ConnectHook] = dataclasses.field(
        default_factory=weakref.WeakValueDictionary,
        repr=False,
    )
    _reader: asyncio.StreamReader | None = dataclasses.field(default=None, repr=False)
    _writer: asyncio.StreamWriter | None = dataclasses.field(default=None, repr=False)

    @classmethod
    async def from_url(cls, url: str, /) -> "typing_extensions.Self":
        """Connect to the provided Redis url."""
        parsed = urllib.parse.urlparse(url)
        if not parsed.hostname or not parsed.port or parsed.scheme != "redis":
            msg = "Only urls of scheme 'redis://host:port' are supported"
            raise ValueError(msg)

        return await cls.from_host_port(parsed.hostname, parsed.port)

    @classmethod
    async def from_host_port(cls, host: str, port: int, /) -> "typing_extensions.Self":
        """Connect to Redis at the provided host and port."""
        self = cls(host=host, port=port)

        async def _set_resp3(con: protocol.ConnectionProto) -> None:
            await con.write_command(command.Command(b"HELLO", _RESP3))
            hello = await con.read_response(disconnect_on_error=True)

            if hello[b"proto"] != _RESP3:
                msg = "Failed to set redis protocol version to 3"
                raise error.RedisError(msg)

        self._post_connect_hooks["HELLO"] = _set_resp3

        await self.connect()
        return self

    def __del__(self) -> None:
        if getattr(self, "_writer", None):
            self._close()

    def _close(self) -> asyncio.StreamWriter:
        assert self._writer

        writer = self._writer
        writer.close()
        self._writer = self._reader = None

        return writer

    def is_alive(self) -> bool:
        """Check whether this connection has an active redis connection."""
        return self._reader is not None and self._writer is not None

    async def connect(self) -> None:
        """Connect to Redis with the connection parameters provided at instantiation."""
        try:
            reader, writer = await asyncio.open_connection(
                self.host,
                self.port,
                limit=self.buffer_limit,
            )
            sock: socket.socket = writer.transport.get_extra_info("socket")
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        except OSError as exc:
            msg = f"Failed to connect to '{self.host}:{self.port}'."
            raise ConnectionError(msg) from exc

        except Exception as exc:
            raise ConnectionError from exc

        self._reader = reader
        self._writer = writer

        for hook in self._post_connect_hooks.values():
            await hook(self)

    async def disconnect(self) -> None:
        """Close the connection with Redis."""
        if not self.is_alive():
            msg = "The connection is already closed."
            raise error.StateError(msg)

        closing_writer = self._close()
        await closing_writer.wait_closed()

    async def write_command(self, command: protocol.CommandProto, /) -> None:
        """Write a command to the connected Redis instance.

        This requires this connection to be alive.

        Either ``read_response`` or ``discard_response`` *must* be called after
        this.
        """
        if not self.is_alive():
            msg = "Cannot send commands to a closed connection."
            raise error.StateError(msg)

        assert self._writer is not None

        try:
            self._writer.write(b"*%i\r\n" % len(command))
            for arg in command:
                self._writer.write(b"$%i\r\n" % len(arg))
                self._writer.write(arg)
                self._writer.write(b"\r\n")

            await self._writer.drain()

        except OSError as exc:
            self._close()

            if len(exc.args) == 1:
                error_code = "UNKNOWN"
                error_msg = exc.args[0]

            else:
                error_code, error_msg, *_ = exc.args

            msg = f"Writing to '{self.host}:{self.port}' raised {error_code}: {error_msg}"
            raise ConnectionError(msg) from exc

        except BaseException:
            self._close()
            raise

    async def _read_bytes(self, n: int) -> bytes:
        assert self._reader is not None
        assert n > 0

        response = await self._reader.read(n + 2)

        if response[-2:] == b"\r\n":
            return response[:-2]

        msg = "reading data from stream returned incomplete response."
        raise ConnectionError(msg)

    async def _read_response(self) -> object:  # noqa: C901, PLR0911, PLR0912
        assert self._reader is not None

        data = await self._reader.readuntil(b"\r\n")
        if not data.endswith(b"\r\n"):
            msg = "reading data from stream returned incomplete response."
            raise ConnectionError(msg)

        # First character is a symbol that determines the data type,
        # the rest is the actual data.
        byte, response = data[:1], data[1:]

        if byte == ByteResponse.SIMPLE_ERROR:
            raise error.ResponseError.from_response(response)

        if byte == ByteResponse.BLOB_ERROR:
            response = await self._read_bytes(int(response))
            raise error.ResponseError.from_response(response)

        if byte == ByteResponse.SIMPLE_STRING:
            return response

        if byte == ByteResponse.BLOB_STRING:
            return await self._read_bytes(int(response))

        if byte == ByteResponse.VERBATIM_STRING:
            # TODO: Maybe store the format instead of discarding it.
            return (await self._read_bytes(int(response)))[4:]

        if byte in (ByteResponse.NUMBER, ByteResponse.BIG_NUMBER):
            return int(response)

        if byte == ByteResponse.DOUBLE:
            return float(response)

        if byte == ByteResponse.BOOLEAN:
            return response == b"t"

        if byte == ByteResponse.NULL:
            return None

        if byte in ByteResponse.ARRAY:
            return [await self._read_response() for _ in range(int(response))]

        if byte in ByteResponse.SET:
            return {await self._read_response() for _ in range(int(response))}

        if byte == ByteResponse.MAP:
            return {
                await self._read_response(): await self._read_response()
                for _ in range(int(response))
            }

        if byte in (ByteResponse.PUSH, ByteResponse.ATTRIBUTE):
            raise NotImplementedError

        msg = f"{byte} is not a valid response type"
        raise error.ResponseError(byte.decode("utf-8", errors="replace"), msg)

    async def read_response(self, *, disconnect_on_error: bool = True) -> typing.Any:  # noqa: ANN401
        """Read the response to a previously executed command.

        This requires this connection to be alive.
        """
        try:
            return await self._read_response()

        except OSError as exc:
            if disconnect_on_error:
                await self.disconnect()

            msg = f"Failed to read from '{self.host}:{self.port}': {exc}"
            raise ConnectionError(msg) from exc

        except BaseException:
            if disconnect_on_error:
                await self.disconnect()

            raise

    async def _discard_response(self) -> None:
        assert self._reader is not None

        data = await self._reader.readuntil(b"\r\n")
        if not data.endswith(b"\r\n"):
            msg = "reading data from stream returned incomplete response."
            raise ConnectionError(msg)

        # First character is a symbol that determines the data type,
        # the rest is the actual data.
        byte, response = data[:1], data[1:]

        if byte in (
            ByteResponse.SIMPLE_ERROR,
            ByteResponse.SIMPLE_STRING,
            ByteResponse.NUMBER,
            ByteResponse.BIG_NUMBER,
            ByteResponse.DOUBLE,
            ByteResponse.BOOLEAN,
            ByteResponse.NULL,
        ):
            return

        if byte in (
            ByteResponse.BLOB_ERROR,
            ByteResponse.BLOB_STRING,
            ByteResponse.VERBATIM_STRING,
        ):
            await self._reader.read(int(response))
            return

        if byte in (
            ByteResponse.ARRAY,
            ByteResponse.SET,
        ):
            await self._discard_response()
            return

        if byte == ByteResponse.MAP:
            await self._discard_response()
            await self._discard_response()
            return

        if byte == ByteResponse.PUSH:
            # Can't just return None here as we actually need to consume the bytes.
            # We need to error until these are implemented.
            raise NotImplementedError

        if byte == ByteResponse.ATTRIBUTE:
            raise NotImplementedError

        # Unknown response type but we're ignoring the response anyway.
        return

    async def discard_response(self, *, disconnect_on_error: bool = True) -> typing.Any:  # noqa: ANN401
        """Discard the response to the previously executed command.

        This requires this connection to be alive.
        """
        try:
            return await self._discard_response()

        except OSError as exc:
            if disconnect_on_error:
                await self.disconnect()

            msg = f"Failed to read from '{self.host}:{self.port}': {exc}"
            raise ConnectionError(msg) from exc

        except BaseException:
            if disconnect_on_error:
                await self.disconnect()

            raise


@dataclasses.dataclass(slots=True)
class ActionableConnection:
    """High-level connection implementation.

    This connection can make connections to Redis, and both send and receive
    commands. This connection implements higher-level commands to make it more
    convenient to run commonly-used commands.

    Only RESP3 connections are supported.
    """

    connection: protocol.ConnectionProto

    @classmethod
    async def from_url(
        cls,
        url: str,
        /,
        *,
        connection_class: type[protocol.ConnectionProto] = Connection,
    ) -> "typing_extensions.Self":
        """Connect to the provided Redis url."""
        parsed = urllib.parse.urlparse(url)
        if not parsed.hostname or not parsed.port or parsed.scheme != "redis":
            msg = "Only urls of scheme 'redis://host:port' are supported"
            raise ValueError(msg)

        return await cls.from_host_port(
            parsed.hostname,
            parsed.port,
            connection_class=connection_class,
        )

    @classmethod
    async def from_host_port(
        cls,
        host: str,
        port: int,
        /,
        *,
        connection_class: type[protocol.ConnectionProto] = Connection,
    ) -> "typing_extensions.Self":
        """Connect to Redis at the provided host and port."""
        connection = await connection_class.from_host_port(host, port)
        return cls(connection)

    async def connect(self) -> None:
        """Connect to Redis with the connection parameters provided at instantiation."""
        await self.connection.connect()

    async def disconnect(self) -> None:
        """Close the connection with Redis."""
        await self.connection.disconnect()

    async def write_command(self, command: protocol.CommandProto, /) -> None:
        """Write a command to the connected Redis instance.

        This requires this connection to be alive.

        Either ``read_response`` or ``discard_response`` *must* be called after
        this.
        """
        await self.connection.write_command(command)

    async def read_response(self, *, disconnect_on_error: bool = True) -> typing.Any:  # noqa: ANN401
        """Read the response to a previously executed command.

        This requires this connection to be alive.
        """
        return await self.connection.read_response(disconnect_on_error=disconnect_on_error)

    async def discard_response(self, *, disconnect_on_error: bool = True) -> None:
        """Discard the response to the previously executed command.

        This requires this connection to be alive.
        """
        return await self.connection.discard_response(disconnect_on_error=disconnect_on_error)

    async def xread(
        self,
        streams: collections.abc.Mapping[str | bytes, str | bytes | int],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> typing.Any:  # noqa: ANN401
        """Read data from one or multiple streams.

        Returns all stream entries after the provided id. Valid ids include any
        actual id, 0 to start at the beginning of the stream, or $ to start at
        the end of the stream (this is only useful when block is set).

        If count is provided, return at most <count> entries.

        If block is provided, wait up to <block> [ms] for one or more entries
        to be added to the stream if it would otherwise return empty. Block can
        be set to 0 to block indefinitely.

        See also: https://redis.io/docs/latest/commands/xread/
        """
        # TODO: Proper xread return type
        cmd = command.Command(b"XREAD")

        if count is not None:
            cmd.arg(b"COUNT").arg(count)

        if block is not None:
            cmd.arg(b"BLOCK").arg(block)

        cmd.arg(b"STREAMS")
        for stream, start_id in streams.items():
            cmd.arg(stream).arg(start_id)

        await self.connection.write_command(cmd)
        return await self.connection.read_response(disconnect_on_error=True)
