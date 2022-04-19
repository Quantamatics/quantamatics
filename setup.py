# coding: utf-8
# !/usr/bin/env python
from setuptools import setup

DISTNAME = 'quantamatics'
DESCRIPTION = "Quantamatics Open Source API Package"
LONG_DESCRIPTION = ""
MAINTAINER = 'Facteus, Inc.'
AUTHOR = 'Facteus Platform team'
AUTHOR_EMAIL = 'support@quantamatics.com'
URL = "https://github.com/Quantamatics/quantamatics"
LICENSE = ""

VERSION = "1.0.0"

packages = ['quantamatics', 'quantamatics/core', 'quantamatics/data', 'quantamatics/providers']
package_data = {'quantamatics': ['*']}

classifiers = ['Development Status :: 5 - Production/Stable',
               'Programming Language :: Python',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: 3.6',
               'Programming Language :: Python :: 3.7',
               'Programming Language :: Python :: 3.8',
               'Programming Language :: Python :: 3.9',
               'Programming Language :: Python :: 3.10',
               'Programming Language :: Python :: 3.11',
               'Intended Audience :: Science/Research',
               'Intended Audience :: Financial and Insurance Industry',
               'Intended Audience :: Customer Service',
               'Topic :: Scientific/Engineering :: Mathematics :: Finance :: Artificial Intelligence',
               'Operating System :: OS Independent']

install_reqs = [
    'setuptools',
    'requests',
    'python-jose[cryptography]',
    'pyOpenSSL>=21.0.0',
    'pandas',
    'numpy',
    'aiohttp[speedups]',
    'nest_asyncio'
]

if __name__ == "__main__":
    setup(
        name=DISTNAME,
        version=VERSION,
        maintainer=MAINTAINER,
        description=DESCRIPTION,
        license=LICENSE,
        url=URL,
        long_description=LONG_DESCRIPTION,
        packages=packages,
        package_data=package_data,
        classifiers=classifiers,
        install_requires=install_reqs
    )
