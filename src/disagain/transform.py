"""Module containing data transformers for high-level Redis commands."""

import collections.abc
import typing

if typing.TYPE_CHECKING:
    import typing_extensions

__all__: collections.abc.Sequence[str] = ("transform_xread",)


XREADResponse: typing.TypeAlias = dict[bytes, list["StreamEntry"]]


def _pairwise_to_dict(arg: collections.abc.Iterable[bytes]) -> dict[bytes, bytes]:
    arg_iter = iter(arg)
    return dict(zip(arg_iter, arg_iter, strict=True))


class StreamEntry(typing.NamedTuple):
    """Wrapper type for Redis stream entries."""

    id: bytes
    data: collections.abc.Mapping[bytes, bytes]

    @classmethod
    def from_raw(cls, raw: typing.Sequence[typing.Any]) -> "typing_extensions.Self":
        """Create a StreamEntry from data returned by redis.

        This expects data of shape:
        ```
        [
            b"entry id",
            [b"entry data key 1", b"entry data value 1", ...]
        ]
        ```
        """
        return cls(raw[0], _pairwise_to_dict(raw[1]))


def transform_xread(data: dict[bytes, typing.Any]) -> XREADResponse:
    """Transform Redis XREAD output into a more easily handleable shape."""
    # Response is of shape
    #
    # dict: stream name -> [entries...]
    #              b        entries: [id, [data...]]
    #                                 b    data: [key 1, value 1, key 2, value 2, ...]
    #                                             b      b        b      b
    # We transform this into
    #
    # dict: stream name -> [entries...]
    #              b        entries: [StreamEntry(id, {data}), ...]
    #                                             b    data: key -> value
    #                                                        b      b
    return {
        stream_name: list(map(StreamEntry.from_raw, entries))
        for stream_name, entries in data.items()
    }
