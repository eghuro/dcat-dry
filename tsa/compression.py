"""Module for handling compressed distribution files."""
import gzip
import logging
from io import BytesIO
from sys import platform
from typing import Generator, Tuple

import libarchive
import requests

from tsa.monitor import monitor

if platform == "darwin":
    import os

    os.environ[
        "LIBARCHIVE"
    ] = "/usr/local/Cellar/libarchive/3.3.3/lib/libarchive.13.dylib"


def _load_data(iri: str, response: requests.Response) -> bytes:
    log = logging.getLogger(__name__)
    log.debug("Downloading %s into an in-memory buffer", iri)
    buffer = BytesIO(response.content)
    log.debug("Read the buffer")
    data = buffer.read()
    log.debug("Size: %d", len(data))
    return data


def decompress_gzip(
    iri: str, response: requests.Response
) -> Generator[Tuple[str, str], None, None]:
    """Decompress gzip data.

    Loads response data in memory, decompresses it as gzip and decodes the result to string.

    :param iri: the IRI to fetch
    :param response: the response to read from
    :return: the decompressed data as a single element generator
    """
    data = _load_data(iri, response)

    if iri.endswith(".gz"):
        iri = iri[:-3]
    else:
        iri = iri + "/data"
    decompressed = BytesIO()
    decompressed.write(gzip.decompress(data))

    deco_size_total = decompressed.getbuffer().nbytes
    monitor.log_size(deco_size_total)
    log = logging.getLogger(__name__)
    log.debug("Done decompression, total decompressed size %d", deco_size_total)
    yield iri, decompressed.getvalue().decode("utf-8")


def _create_sub_iri(name: str, iri: str, log: logging.Logger) -> str:
    if len(name) == 0:
        if iri.endswith(".zip"):
            sub_iri = iri[:-4]
        else:
            sub_iri = f"{iri}/{name}"
            log.error("Empty name, iri: %s", iri)
    else:
        sub_iri = f"{iri}/{name}"
    return sub_iri


def _get_name(entry: libarchive.entry.ArchiveEntry) -> str:
    try:
        name = str(entry)
    except TypeError:
        name = ""
    return name


def _load_entry_data(entry: libarchive.entry.ArchiveEntry) -> Tuple[int, BytesIO]:
    conlen = 0
    data = BytesIO()
    for block in entry.get_blocks():
        data.write(block)
        conlen = conlen + len(block)
    return conlen, data


def decompress_7z(
    iri: str, response: requests.Response
) -> Generator[Tuple[str, str], None, None]:
    """Download a 7z file, decompress it and decode results to string.

    :param iri: the IRI to fetch
    :param response: the response to read from
    :return: the decompressed data as a generator of (iri, decompressed data) pairs
    """
    compressed_data = _load_data(iri, response)
    log = logging.getLogger(__name__)

    deco_size_total = 0
    with libarchive.memory_reader(compressed_data) as archive:
        for entry in archive:
            name = _get_name(entry)
            sub_iri = _create_sub_iri(name, iri, log)
            conlen, data = _load_entry_data(entry)
            monitor.log_size(conlen)
            log.debug("Subfile has size %d", conlen)
            deco_size_total = deco_size_total + conlen
            if conlen > 0:
                try:
                    yield sub_iri, data.getvalue().decode("utf-8")
                except UnicodeDecodeError:
                    yield sub_iri, str(data.getvalue())
    log.debug("Done decompression, total decompressed size %d", deco_size_total)
