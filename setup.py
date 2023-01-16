#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-mailchimp-export",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Simon Data",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_mailchimp_export"],
    install_requires=[
        "singer-python==5.6.1",
        "mailsnake==1.6.4",
        'requests==2.20.0',
        "simplejson==3.11.1",
        "pendulum==1.2.0",
        "urllib3==1.22",
    ],
    entry_points="""
    [console_scripts]
    tap-mailchimp-export=tap_mailchimp_export:main
    """,
    packages=["tap_mailchimp_export"],
    package_data = {
        "schemas": ["tap_mailchimp_export/schemas/*.json"]
    },
    include_package_data=True,
)
