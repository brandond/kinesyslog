kinesyslog
==========
[![PyPI version](https://badge.fury.io/py/kinesyslog.svg)](https://badge.fury.io/py/kinesyslog)
[![Build Status](https://travis-ci.org/brandond/kinesyslog.svg?branch=master)](https://travis-ci.org/brandond/kinesyslog)

Syslog and GELF relay to AWS Kinesis Firehose. Supports UDP, TCP, and TLS; RFC3164, RFC5424, RFC5425, RFC6587, GELF v1.1.

Prerequisites
-------------

This module requires Python 3.5 or better, due to its use of the ``asyncio`` framework.

This module will make use of [python-libuuid](https://pypi.org/project/python-libuuid/) for fast UUID generation, if it is available.

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
  --stream TEXT             Kinesis Firehose Delivery Stream Name.  [required]
  --address TEXT            Bind address.  [default: 0.0.0.0]
  --udp-port INTEGER        Bind port for UDP listener; 0 to disable. May be repeated.  [default: 0]
  --tcp-port INTEGER        Bind port for TCP listener; 0 to disable. May be repeated.  [default: 0]
  --tls-port INTEGER        Bind port for TLS listener; 0 to disable. May be repeated.  [default: 6514]
  --cert PATH               Certificate file for TLS listener.
  --key PATH                Private key file for TLS listener.
  --proxy-protocol INTEGER  Enable PROXY protocol v1/v2 support on the selected TCP or TLS port; 0 to disable. May be repeated.  [default: 0]
  --spool-dir DIRECTORY     Spool directory for compressed records prior to upload.  [default: /tmp]
  --region TEXT             The region to use. Overrides config/env settings.
  --profile TEXT            Use a specific profile from your credential file.
  --gelf                    Listen for messages in Graylog Extended Log Format (GELF) instead of Syslog.
  --group-prefix TEXT       Use the specified LogGroup prefix.  [default: /kinesyslog]
  --debug                   Enable debug logging to STDERR.
  --help                    Show this message and exit.
```

Options can also be passed via environment variables. Repeated option values should be separated by whitespace.
```sh
export KINESYSLOG_GELF="1"
export KINESYSLOG_TLS_PORT="0"
export KINESYSLOG_UDP_PORT="12201"
export KINESYSLOG_TCP_PORT="12201 12202"
export KINESYSLOG_STREAM="kinesyslog-gelf-stream"

kinesyslog listen
```

Installation
------------

Follow standard steps to install Python packages using `pip`.  It is recommended that you use a virtualenv:

```sh
virtualenv /opt/kinesyslog
source /opt/kinesyslog/bin/activate
pip install --upgrade pip
pip install kinesyslog
```

Once kinesyslog has been installed, you can generate and install a systemd unit. You will likely need to run the install command with sudo or as root, and provide the full path to the executable if it is in a virtualenv: ```sudo `which kinesyslog` install```

```
Usage: kinesyslog install [OPTIONS]

Options:
  --user TEXT             Configure the service unit to run as specified user.
                          If not specified, the service runs as root.
  --system-dir DIRECTORY  Install the service unit to the specified systemd
                          system directory  [default: /etc/systemd/system]
  --help                  Show this message and exit.
```

You should run `systemctl edit kinesyslog` to adjust the service configuration variable specific to your environment - ports, certificates, streams, etc.

If not running on EC2, you will need to ensure that credentials are available to the service; the easiest way to do this is to install `awscli` and run `aws configure` as the user that the service will run as.

Record Format
-------------

When delivered to S3, the objects will contain multiple concatenated GZip-compressed records in JSON format. The records are near-identical to those created by [CloudWatch Logs subscriptions](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html#FirehoseExample) with a few caveats:
* The 'logGroup' and 'subscriptionFilter' fields are set to `<PREFIX>/<FORMAT>/<PORT>`, where:
  * `<PREFIX>` is the specified prefix; '/kinesyslog' by default.
  * `<FORMAT>` is the message format, either 'syslog' or 'gelf'.
  * `<PORT>` is the TCP or UDP port on which the message was received.
* The 'logStream' field contains the IP address that the message was received from. Note that this is probably NOT the same as the 'source' field in the payload, since that's (hopefully) a FQDN.
* The 'id' field is a GUID instead of a numeric string.


**Sample Record**
```json
{
   "owner" : "123456789012",
   "logGroup" : "/kinesyslog/syslog/514",
   "logStream" : "127.0.0.1",
   "subscriptionFilters" : [
      "/kinesyslog/syslog/514"
   ],
   "messageType" : "DATA_MESSAGE",
   "logEvents" : [
      {
         "id" : "363f3136-9356-443b-9eee-ce72d32d2307",
         "timestamp" : 1519247270240,
         "message" : "<13>1 2018-02-21T21:07:50.239881+00:00 host.example.com user 4326 - [timeQuality tzKnown=\"1\" isSynced=\"0\"] Hello, World!"
      },
      {
         "id" : "fae5ea37-972a-4bc5-a259-0d5404680758",
         "timestamp" : 1519247271713,
         "message" : "<13>1 2018-02-21T21:07:51.712636+00:00 host.example.com user 4327 - [timeQuality tzKnown=\"1\" isSynced=\"0\"] I, for one, welcome our new insect overlords"
      }
   ]
}
```

Data Flow
---------

Syslog events are received via UDP, TCP, or TLS and stored in an in-memory buffer on the main listener process. When this buffer reaches a preset threshold of size (4 MB) or message age (60 seconds), the message buffer is handed off to a background worker for processing, and a new buffer allocated. 4MB has been found to frequently compress down to near 1000 KB, which is the maximum record size for Kinesis Firehose.

The background worker process extracts event timestamps, assigns events a unique ID, and serializes the resulting events into a JSON document that is GZip-compressed and written to a spool file, which will comprise a single Firehose record. The buffer will be split into multiple records (along message boundaries) if its compressed size exceeds the maximum Firehose record size. Every 60 seconds, all pending records are flushed to Firehose in batches that do not exceed 4 MB or 500 records. Spooled records are removed only when Firehose acknowledges successful upload by assigning a Record ID.

Todo
----

* Client certificate validation
