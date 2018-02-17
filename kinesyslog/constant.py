from struct import Struct

MAX_MESSAGE_LENGTH = 1024 * 16  # Maximum supported message length
LESSTHAN = 0x3C  # First char of non-octet-counted syslog message
OPENBRACKET = 0x7B  # First char of GELF message
DIGITS = bytearray(i for i in range(0x30, 0x3A))  # First char of octet-counted syslog message
TERMS = bytearray([0x00, 0x0A, 0x0D])  # Framed message terminators
METHODS = bytearray([0x47, 0x48])  # First char of common HTTP methods
ZLIB_MAGIC = b'\x78'  # ZLIB magic
GZIP_MAGIC = b'\x1F\x8B'  # GZIP magic
GELF_MAGIC = b'\x1E\x0F'  # Chunked GELF header magic
GELF_HEADER = Struct('!HqBB')  # Chunked GELF header format
FLUSH_TIME = 60  # Sink buffer max message age
FLUSH_SIZE = 1024 * 1024 * 4  # Sink buffer max size
MAX_RECORD_SIZE = 1024 * 1000  # Maximum Firehose record size
TIMER_INTERVAL = 10  # Sink buffer autoflush check interval
PROXY10_SEP = b'\x20'  # Proxy Protocol v1.0 separator
PROXY10_TERM = b'\x0D\x0A'  # Proxy Protocol v1.0 terminator
PROXY10_MAGIC = b'PROXY\x20'  # Proxy Protocol v1.0 header
PROXY20_MAGIC = b'\x0D\x0A\x0D\x0A\x00\x0D\x0AQUIT\x0A'  # Proxy Protocol v2.0 header
