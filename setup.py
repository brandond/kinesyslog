# -*- coding: utf-8 -*-
from os import chdir
from os.path import abspath, dirname

from setuptools import find_packages, setup

chdir(dirname(abspath(__file__)))

version = {}

with open('README.rst') as f:
    readme = f.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    author='Brandon Davidson',
    author_email='brad@oatmail.org',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: No Input/Output (Daemon)',
        'Framework :: AsyncIO',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet :: Log Analysis',
        'Topic :: System :: Logging',
    ],
    description='Syslog and GELF relay to AWS Kinesis Firehose. Supports UDP, TCP, and TLS; RFC3164, RFC5424, RFC5425, RFC6587, GELF v1.1.',
    entry_points={
        'console_scripts': ['kinesyslog=kinesyslog.commands:cli']
    },
    extras_require={
        'dev': [
            'setuptools-version-command',
        ],
        'prometheus': [
            'aiohttp >= 3.5.4',
            'aioprometheus >= 18.7.1',
        ],
        'all': [
            'libuuid >= 1.0.0',
            'setproctitle >= 1.1.10',
            'aiohttp >= 3.5.4',
            'aioprometheus >= 18.7.1',
        ],
    },
    include_package_data=True,
    install_requires=requirements,
    long_description=readme,
    name='kinesyslog',
    packages=find_packages(exclude=('docs')),
    python_requires='>=3.5',
    url='https://github.com/brandond/kinesyslog',
    version_command=('git describe --tags --dirty', 'pep440-git-full'),
)
