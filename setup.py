#!/usr/bin/env python3

from setuptools import find_packages, setup

with open("README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

EXCLUDE_FROM_PACKAGES = []

setup(
    name='StudySpark',
    version='0.0.1',
    author='wanghuagang',
    author_email="huagang517@126.com",
    description="Study Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=EXCLUDE_FROM_PACKAGES),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],
    entry_points={}
)
