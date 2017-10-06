kinesyslog
==========
[![PyPI version](https://badge.fury.io/py/kinesyslog.svg)](https://badge.fury.io/py/kinesyslog)

Syslog and GELF relay to AWS Kinesis Firehose. Supports UDP, TCP, and TLS; RFC2164, RFC5424, RFC5425, RFC6587, GELF v1.1.

Prerequisites
-------------

This module requires Python 3.5 or better, due to its use of the ``asyncio`` framework.

This module uses Boto3 to make API calls against the Kinesis Firehose service. You
should have a working AWS API environment (~/.aws/credentials,
environment variables, or EC2 IAM Role) that allows calling Kinesis Firehose's
``put-record-batch`` method against the account that it is running in, and the stream
specified on the command line.

Usage
-----

```
Usage: kinesyslog listen [OPTIONS]

Options:
  --stream TEXT          Kinesis Firehose Delivery Stream Name.  [required]
  --address TEXT         Bind address.  [default: 0.0.0.0]
  --port INTEGER         Bind port for TLS listener; 0 to disable.  [default: 6514]
  --cert PATH            Certificate file for TLS listener.  [default: localhost.crt]
  --key PATH             Private key file for TLS listener.  [default: localhost.key]
  --tcp-port INTEGER     Bind port for TCP listener; 0 to disable.  [default: 0]
  --udp-port INTEGER     Bind port for UDP listener; 0 to disable.  [default: 0]
  --spool-dir DIRECTORY  Spool directory for compressed records prior to upload.  [default: /tmp]
  --region TEXT          The region to use. Overrides config/env settings.
  --profile TEXT         Use a specific profile from your credential file.
  --gelf                 Listen for messages in Graylog Extended Log Format (GELF) instead of Syslog.
  --debug                Enable debug logging to STDERR.
  --help                 Show this message and exit.
```

Record Format
-------------

When delivered to S3, the objects will contain multiple concatenated GZip-compressed records in JSON format. The records are near-identical to those created by CloudWatch Logs subscriptions. The 'logGroup', 'logStream', and 'subscriptionFilter' fields are set to the message type (syslog or gelf), and the ID field is a GUID instead of a numeric string. Event timestamps are floats instead of ints.

Data Flow
---------

Syslog events are received via UDP, TCP, or TLS and stored in an in-memory buffer on the main listener process. When this buffer reaches a preset threshold of size (4 MB) or message age (60 seconds), the message buffer is handed off to a background worker for processing, and a new buffer allocated. 4MB has been found to frequently compress down to near 1000 KB, which is the maximum record size for Kinesis Firehose.

The background worker process extracts event timestamps, assigns events a unique ID, and serializes the resulting events into a JSON document that is GZip-compressed and written to a spool file, which will comprise a single Firehose record. The buffer will be split into multiple records (along message boundaries) if its compressed size exceeds the maximum Firehose record size. Every 60 seconds, all pending records are flushed to Firehose in batches that do not exceed 4 MB or 500 records. Spooled records are removed only when Firehose acknowledges successful upload by assigning a Record ID.

Todo
----

* Client certificate validation
