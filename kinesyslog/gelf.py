import logging
import time
from collections import namedtuple

from . import constant

Header = namedtuple('Header', 'magic id number count')
logger = logging.getLogger(__name__)


class ChunkedMessage(object):
    __slots__ = ['header', 'payload', 'parts', 'found', 'seen']

    def __init__(self, data):
        self.header = self.parse_header(data)
        self.payload = data[12:]
        self.parts = None
        self.found = 0
        self.seen = 0
        assert self.header.count <= 128

    def __hash__(self):
        return self.header.id

    def __eq__(self, other):
        return self.header.id == other.header.id

    def __ne__(self, other):
        return self.header.id != other.header.id

    def is_complete(self):
        return self.header.count == self.found

    def update(self, other=None):
        if other:
            assert other.header.id == self.header.id
        else:
            other = self
        if not self.parts:
            self.parts = [None] * self.header.count
        if not self.parts[other.header.number]:
            self.parts[other.header.number] = other.payload
            self.seen = time.time()
            self.found += 1
        if self.is_complete():
            return b''.join(self.parts)

    @classmethod
    def parse_header(cls, data):
        return Header(*constant.GELF_HEADER.unpack(data[0:12]))
