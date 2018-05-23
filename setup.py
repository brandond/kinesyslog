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
    name='kinesyslog',
    version_command=('git describe --tags --dirty', 'pep440-git-full'),
    description='Syslog and GELF relay to AWS Kinesis Firehose. Supports UDP, TCP, and TLS; RFC3164, RFC5424, RFC5425, RFC6587, GELF v1.1.',
    long_description=readme,
    author='Brandon Davidson',
    author_email='brad@oatmail.org',
    url='https://github.com/brandond/kinesyslog',
    license='Apache',
    packages=find_packages(exclude=('docs')),
    entry_points={
        'console_scripts': ['kinesyslog=kinesyslog.commands:cli']
    },
    include_package_data=True,
    install_requires=requirements,
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet :: Log Analysis',
        'Topic :: System :: Logging',
    ],
    extras_require={
        'dev': [
            'setuptools-version-command',
        ],
        'libuuid': [
            'python-libuuid',
        ]
    },
)
