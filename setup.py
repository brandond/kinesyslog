# -*- coding: utf-8 -*-

import sys
from setuptools import setup, find_packages

version = {}

with open("kinesyslog/version.py") as fp:
    exec(fp.read(), version)

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='kinesyslog',
    version=version['__version__'],
    description='Syslog relay to Kinesis Firehose. Supports UDP, TCP, and TLS: RFC2164, RFC5424, RFC5425, RFC6587.',
    long_description=readme,
    author='Brandon Davidson',
    author_email='brad@oatmail.org',
    url='https://github.com/brandond/kinesyslog',
    download_url='https://github.com/brandond/kinesyslog/tarball/{}'.format(version['__version__']),
    license=license,
    packages=find_packages(exclude=('docs')),
    entry_points={
        'console_scripts': ['kinesyslog=kinesyslog.commands:cli']
    },
    include_package_data=True,
    install_requires=requirements,
    python_requires='>=3.5',
)
