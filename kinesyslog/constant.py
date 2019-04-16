from struct import Struct

MAX_MESSAGE_COUNT = 128  # Max messages per ring buffer batch
MAX_SOURCE_LENGTH = 16  # Maximum length of a source address string
MAX_MESSAGE_LENGTH = 1024 * 6  # Maximum supported message length
MAX_MESSAGE_BUFFER = MAX_MESSAGE_LENGTH * 48  # Maximum RX buffer size
DIGITS = bytearray(i for i in range(0x30, 0x3A))  # First char of octet-counted syslog message
TERMS = bytearray([0x0A, 0x00, 0x0D])  # Framed message terminators
ZLIB_MAGIC = b'\x78'  # ZLIB magic
GZIP_MAGIC = b'\x1F\x8B'  # GZIP magic
GELF_MAGIC = b'\x1E\x0F'  # Chunked GELF header magic
GELF_HEADER = Struct('!HqBB')  # Chunked GELF header format
FLUSH_TIME = 60  # Sink buffer max message age
FLUSH_SIZE = 1024 * 1024 * 4  # Sink buffer max size
MAX_RECORD_SIZE = 1024 * 1000  # Maximum Firehose record size
TIMER_INTERVAL = 10  # Sink buffer autoflush check interval
MAX_BATCH_COUNT = 500  # Max Firehose records per batch
MAX_BATCH_SIZE = 1024 * 1024 * 4  # Max cumulative Firehose record size per batch
SPOOL_PREFIX = 'firehose_event-'  # Spool file prefix
TEMP_PREFIX = '_temp_'  # Temp file prefix
PROXY10_SEP = b'\x20'  # Proxy Protocol v1.0 separator
PROXY10_TERM = b'\x0D\x0A'  # Proxy Protocol v1.0 terminator
PROXY10_MAGIC = b'PROXY\x20'  # Proxy Protocol v1.0 header
PROXY20_MAGIC = b'\x0D\x0A\x0D\x0A\x00\x0D\x0AQUIT\x0A'  # Proxy Protocol v2.0 header
PROXY20_COMMANDS = {0x00: 'local',
                    0x01: 'proxy',
                    }
PROXY20_FAMILIES = {0x10: 'inet',
                    0x20: 'inet6',
                    0x30: 'unix',
                    }
PROXY20_PROTOCOLS = {0x01: 'stream',
                     0x02: 'datagram',
                     }
PROXY20_TLV_TYPES = {0x01: 'PP2_TYPE_ALPN',
                     0x02: 'PP2_TYPE_AUTHORITY',
                     0x03: 'PP2_TYPE_CRC32C',
                     0x04: 'PP2_TYPE_NOOP',
                     0x20: 'PP2_TYPE_SSL',
                     0x26: 'PP2_TYPE_NETNS',
                     0xEA: 'PP2_TYPE_AWS',
                     }
STAT_HTTP_REQS = 'kinesyslog_http_requests_total'
STAT_MESSAGE_BYTES = 'kinesyslog_message_bytes_total'
STAT_MESSAGE_COUNT = 'kinesyslog_message_count_total'
STAT_BATCH_FAILED = 'kinesyslog_batch_record_failed'
STAT_BATCH_RECORDS = 'kinesyslog_batch_records'
STAT_BATCH_BYTES = 'kinesyslog_batch_bytes'
STAT_RECORD_BYTES = 'kinesyslog_record_bytes'
STAT_LISTENERS = 'kinesyslog_listener_count'
STAT_SPOOL_COUNT = 'kinesyslog_spool_count'
STAT_SPOOL_AGE = 'kinesyslog_spool_age'
