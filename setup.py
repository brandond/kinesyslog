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
    description='Syslog to AWS Kinesis Firehose gateway',
    long_description=readme,
    author='Brandon Davidson',
    author_email='brad@oatmail.org',
    url='https://github.com/brandond/aws-acl-helper',
    download_url='https://github.com/brandond/aws-acl-helper/tarball/{}'.format(version['__version__']),
    license=license,
    packages=find_packages(exclude=('docs')),
    entry_points={
        'console_scripts': ['kinesyslog=kinesyslog.commands:cli']
    },
    include_package_data=True,
    install_requires=requirements,
)
