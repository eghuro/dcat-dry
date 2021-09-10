"""Module for handling compressed distribution files."""
import gzip
import logging
from io import BytesIO
from sys import platform
from typing import Generator, Tuple

import libarchive
import requests
from jedi.api.interpreter import _create

from tsa.monitor import monitor

if platform == 'darwin':
    import os
    os.environ['LIBARCHIVE'] = '/usr/local/Cellar/libarchive/3.3.3/lib/libarchive.13.dylib'


def _load_data(iri: str, request: requests.Request) -> BytesIO:
    log = logging.getLogger(__name__)
    log.debug(f'Downloading {iri} into an in-memory buffer')
    buffer = BytesIO(request.content)
    log.debug('Read the buffer')
    data = buffer.read()
    log.debug(f'Size: {len(data)}')
    return data


def decompress_gzip(iri: str, request: requests.Request) -> Tuple[str, str]:
    """Decompress gzip data.

    Loads request data in memory, decompresses it as gzip and decodes the result to string.
    """
    data = _load_data(iri, request)

    if iri.endswith('.gz'):
        iri = iri[:-3]
    else:
        iri = iri + '/data'
    decompressed = BytesIO()
    decompressed.write(gzip.decompress(data))

    deco_size_total = decompressed.getbuffer().nbytes
    monitor.log_size(deco_size_total)
    log = logging.getLogger(__name__)
    log.debug(f'Done decompression, total decompressed size {deco_size_total}')
    return f'{iri}', decompressed.getvalue().decode('utf-8')


def _create_sub_iri(name: str, iri: str, log: logging.Logger) -> str:
    if len(name) == 0:
        if iri.endswith('.zip'):
            sub_iri = iri[:-4]
        else:
            sub_iri = f'{iri}/{name}'
            log.error(f'Empty name, iri: {iri!s}')
    else:
        sub_iri = f'{iri}/{name}'
    return sub_iri


def _get_name(entry: libarchive.entry.ArchiveEntry) -> str:
    try:
        name = str(entry)
    except TypeError:
        name = ''
    return name


def _load_entry_data(entry: libarchive.entry.ArchiveEntry) -> Tuple[int, BytesIO]:
    conlen = 0
    data = BytesIO()
    for block in entry.get_blocks():
        data.write(block)
        conlen = conlen + len(block)
    return conlen, data


def decompress_7z(iri: str, request: requests.Request) -> Generator[Tuple[str, str], None, None]:
    """Download a 7z file, decompress it and store contents in redis."""
    data = _load_data(iri, request)
    log = logging.getLogger(__name__)

    deco_size_total = 0
    with libarchive.memory_reader(data) as archive:
        for entry in archive:
            name = _get_name(entry)
            sub_iri = _create_sub_iri(name, iri, log)
            conlen, data = _load_entry_data(entry)
            monitor.log_size(conlen)
            log.debug(f'Subfile has size {conlen}')
            deco_size_total = deco_size_total + conlen
            if conlen > 0:
                try:
                    yield sub_iri, data.getvalue().decode('utf-8')
                except UnicodeDecodeError:
                    yield sub_iri, data.getvalue()
    log.debug(f'Done decompression, total decompressed size {deco_size_total}')
