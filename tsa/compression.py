"""Module for handling compressed distribution files."""
import logging
import uuid

from sys import platform
if platform == 'darwin':
    import os
    os.environ['LIBARCHIVE'] = '/usr/local/Cellar/libarchive/3.3.3/lib/libarchive.13.dylib'

import libarchive
import gzip
from io import BytesIO

from tsa.monitor import monitor
from tsa.redis import data as data_key, MAX_CONTENT_LENGTH, expiration as expire_table, KeyRoot


class SizeException(BaseException):
    """Indicating a subfile is too large."""

    def __init__(self, name):
        """Record the file name."""
        self.name = name

def load_data(iri, r):
    log = logging.getLogger(__name__)
    log.debug(f'Downloading {iri} into an in-memory buffer')
    fp = BytesIO(r.content)
    log.debug(f'Read the buffer')
    data = fp.read()
    log.debug(f'Size: {len(data)}')
    return data


def decompress_gzip(iri, r, red):
    data = load_data(iri, r)

    expiration = expire_table[KeyRoot.DATA]
    if iri.endswith('.gz'):
        iri = iri[:-3]
    else:
        iri = iri + '/data'
    key = data_key(iri)
    decompressed = gzip.decompress(data)
    if len(decompressed) > MAX_CONTENT_LENGTH:
        raise SizeException(name)

    deco_size_total = red.set(key, decompressed)
    red.expire(key, expiration)
    monitor.log_size(deco_size_total)
    log = logging.getLogger(__name__)
    log.debug(f'Done decompression, total decompressed size {deco_size_total}')
    return f'{iri}'


def decompress_7z(iri, r, red):
    """Download a 7z file, decompress it and store contents in redis."""
    data = load_data(iri, r)
    log = logging.getLogger(__name__)

    expiration = expire_table[KeyRoot.DATA]
    deco_size_total = 0
    with libarchive.memory_reader(data) as archive:
        for entry in archive:
            try:
                name = str(entry)
            except:
                name = str(uuid.uuid4())
            if len(name) == 0:
                if iri.endswith('.zip'):
                    sub_iri = iri[:-4]
                else:
                    sub_iri = f'{iri}/{name}'
                    log.error(f'Empty name, iri: {iri!s}')
            else:
                sub_iri = f'{iri}/{name}'
            sub_key = data_key(sub_iri)
            log.debug(f'Store {name} into {sub_key}')
            conlen = 0
            if not red.exists(sub_key):
                red.sadd('purgeable', sub_key)
                for block in entry.get_blocks():
                    if len(block) + conlen > MAX_CONTENT_LENGTH:
                        # Will fail due to redis limitation
                        red.expire(sub_key, 0)
                        raise SizeException(name)

                    red.append(sub_key, block)
                    conlen = conlen + len(block)
                red.expire(sub_key, expiration)
                monitor.log_size(conlen)
                log.debug(f'Subfile has size {conlen}')
                deco_size_total = deco_size_total + conlen
            else:
                log.warn(f'Data already exists for {sub_iri}')
            if conlen > 0:
                yield sub_iri
    log.debug(f'Done decompression, total decompressed size {deco_size_total}')
