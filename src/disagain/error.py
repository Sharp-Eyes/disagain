import collections.abc
import dataclasses

__all__: collections.abc.Sequence[str] = (
    "RedisError",
    "ConnectionError",
    "StateError",
    "ResponseError",
)


class RedisError(Exception):
    ...


class ConnectionError(RedisError):
    ...


class StateError(RedisError):
    ...


@dataclasses.dataclass
class ResponseError(RedisError):
    code: str
    message: str

    def __str__(self) -> str:
        return self.message
    
    @classmethod
    def from_response(cls, response: bytes) -> "ResponseError":
        code, message = response.decode("utf-8", errors="replace").split(" ", 1)
        return cls(code, message)
