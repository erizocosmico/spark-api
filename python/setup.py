from __future__ import with_statement
from setuptools import setup

with open('version.txt', 'r') as f:
    __version__ = f.read().strip()

setup(
    name="test-sourced-spark-api",
    description="API to use Spark on top of source code repositories.",
    version=__version__,
    license="Apache-2.0",
    author="source{d}",
    author_email="hello@sourced.tech",
    url="https://github.com/src-d/spark-api/tree/master/python",
    packages=['sourced',
              'sourced.spark'],
    install_requires=["pyspark>=2.0.0"],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6"
    ]
)
