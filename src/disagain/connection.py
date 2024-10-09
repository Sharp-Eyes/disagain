import asyncio
import collections.abc
import dataclasses
import enum
import typing
import urllib.parse
import socket
import weakref

from disagain import command, error, protocol

__all__: collections.abc.Sequence[str] = ("Connection", "ActionableConnection",)


ConnectHook: typing.TypeAlias = typing.Callable[["Connection"], typing.Coroutine[typing.Any, typing.Any, None]]


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
    async def from_url(cls, url: str, /):
        parsed = urllib.parse.urlparse(url)
        if not parsed.hostname or not parsed.port or parsed.scheme != "redis":
            msg = "Only urls of scheme 'redis://host:port' are supported"
            raise ValueError(msg)

        self = cls(
            host=parsed.hostname,
            port=parsed.port,
        )

        async def _set_resp3(con: protocol.ConnectionProto) -> None:
            await con.write_command(command.Command(b"HELLO", b"3"))
            hello = await con.read_response(disconnect_on_error=True)

            if hello[b"proto"] != 3:
                msg = "Failed to set redis protocol version to 3"
                raise error.RedisError(msg)

        self._post_connect_hooks["HELLO"] = _set_resp3

        await self.connect()
        return self
    

    def __del__(self):
        if getattr(self, "_writer", None):
            self._close()

    def _close(self):
        if self._writer:
            self._writer.close()

        self._writer = self._reader = None

    def is_connected(self) -> bool:
        return self._reader is not None and self._writer is not None

    def _assert_connected(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._reader is None or self._writer is None:
            raise error.StateError
        
        return self._reader, self._writer

    async def connect(self) -> None:
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
        if not self.is_connected():
            return

        assert self._writer is not None

        await self._writer.wait_closed()
        self._reader = self._writer = None
    
    async def write_command(self, command: protocol.CommandProto, /) -> None:
        if not self.is_connected():
            msg = "Cannot send commands to a disconnected client."
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

    async def _read_response(self) -> object:
        assert self._reader is not None

        data = await self._reader.readuntil(b"\r\n")
        if not data.endswith(b"\r\n"):
            msg = "reading data from stream returned incomplete response."
            raise ConnectionError(msg)

        elif data == b"\r\n":
            return await self._read_response()

        # First character is a symbol that determines the data type,
        # the rest is the actual data.
        byte, response = data[:1], data[1:]

        if byte == ByteResponse.SIMPLE_ERROR:
            raise error.ResponseError.from_response(response)
            
        if byte == ByteResponse.BLOB_ERROR:
            response = await self._reader.read(int(response))
            raise error.ResponseError.from_response(response)

        elif byte == ByteResponse.SIMPLE_STRING:
            return response
        
        elif byte == ByteResponse.BLOB_STRING:
            return await self._reader.read(int(response))

        elif byte == ByteResponse.VERBATIM_STRING:
            # TODO: Maybe store the format instead of discarding it.
            return (await self._reader.read(int(response)))[4:]
        
        elif byte in (ByteResponse.NUMBER, ByteResponse.BIG_NUMBER):
            return int(response)
        
        elif byte == ByteResponse.DOUBLE:
            return float(response)
        
        elif byte == ByteResponse.BOOLEAN:
            return response == b"t"

        elif byte == ByteResponse.NULL:
            return None

        elif byte in ByteResponse.ARRAY:
            return [
                await self._read_response() for _ in range(int(response))
            ]

        elif byte in ByteResponse.SET:
            return {
                await self._read_response() for _ in range(int(response))
            }

        elif byte == ByteResponse.MAP:
            return {
                await self._read_response(): await self._read_response()
                for _ in range(int(response))
            }
        
        elif byte == ByteResponse.PUSH:
            raise NotImplementedError

        elif byte == ByteResponse.ATTRIBUTE:
            raise NotImplementedError

        else:
            msg = f"{byte} is not a valid response type"
            raise error.ResponseError(byte.decode("utf-8", errors="replace"), msg)

    async def read_response(self, *, disconnect_on_error: bool = True) -> typing.Any:
        try:
            return await self._read_response()

        except OSError as exc:
            if disconnect_on_error:
                self._close()

            msg = f"Failed to read from '{self.host}:{self.port}': {exc}"
            raise ConnectionError(msg) from exc
        
        except BaseException:
            if disconnect_on_error:
                self._close()

            raise


@dataclasses.dataclass
class ActionableConnection:
    connection: protocol.ConnectionProto

    @classmethod
    async def from_url(
        cls,
        url: str,
        /,
        *,
        connection_class: type[protocol.ConnectionProto] = Connection,
    ) -> "ActionableConnection":
        connection = await connection_class.from_url(url)
        return cls(connection)

    async def connect(self) -> None:
        await self.connection.connect()

    async def disconnect(self) -> None:
        await self.connection.disconnect()

    async def write_command(self, command: protocol.CommandProto) -> None:
        await self.connection.write_command(command)

    async def read_response(self, disconnect_on_error: bool = True) -> typing.Any:
        return await self.connection.read_response(disconnect_on_error=disconnect_on_error)

    async def xread(
        self,
        streams: collections.abc.Mapping[str | bytes, str | bytes | int],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> typing.Any:
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
