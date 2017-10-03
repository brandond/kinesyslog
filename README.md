KINESYSLOG
==========

Syslog relay to AWS Kinesis Firehose. Supports UDP, TCP, and TLS: RFC2164, RFC5424, RFC5425, RFC6587.

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
  --debug                Enable debug logging to STDERR.
  --help                 Show this message and exit.
```

Record Format
-------------

When delivered to S3, the objects will contain multiple concatenated GZip-compressed records in JSON format. The records are near-identical to those created by CloudWatch Logs subscriptions. The 'owner', 'logGroup', 'logStream', and 'subscriptionFilter' fields all have bogus data, and the ID field is a GUID in stead of a numeric string. Event timestamps are floats instead of ints.

Todo
----

* Client certificate validation
* Improved handling of messages with no timestamp/hostname in header
* Support for additional log formats (CEF, Greylog, etc)
