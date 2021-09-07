"""Module for handling compressed distribution files."""
import gzip
import logging
from io import BytesIO
from sys import platform

import libarchive

from tsa.monitor import monitor

if platform == 'darwin':
    import os
    os.environ['LIBARCHIVE'] = '/usr/local/Cellar/libarchive/3.3.3/lib/libarchive.13.dylib'


class SizeException(Exception):
    """Indicating a subfile is too large."""

    def __init__(self, name):
        """Record the file name."""
        self.name = name


def load_data(iri, r):
    log = logging.getLogger(__name__)
    log.debug(f'Downloading {iri} into an in-memory buffer')
    fp = BytesIO(r.content)
    log.debug('Read the buffer')
    data = fp.read()
    log.debug(f'Size: {len(data)}')
    return data


def decompress_gzip(iri, r):
    data = load_data(iri, r)

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


def decompress_7z(iri, r):
    """Download a 7z file, decompress it and store contents in redis."""
    data = load_data(iri, r)
    log = logging.getLogger(__name__)

    deco_size_total = 0
    with libarchive.memory_reader(data) as archive:
        for entry in archive:
            name = str(entry)

            if len(name) == 0:
                if iri.endswith('.zip'):
                    sub_iri = iri[:-4]
                else:
                    sub_iri = f'{iri}/{name}'
                    log.error(f'Empty name, iri: {iri!s}')
            else:
                sub_iri = f'{iri}/{name}'
            conlen = 0
            data = BytesIO()
            for block in entry.get_blocks():
                data.write(block)
                conlen = conlen + len(block)
            monitor.log_size(conlen)
            log.debug(f'Subfile has size {conlen}')
            deco_size_total = deco_size_total + conlen
            if conlen > 0:
                yield sub_iri, data.getvalue().decode('utf-8')
    log.debug(f'Done decompression, total decompressed size {deco_size_total}')
