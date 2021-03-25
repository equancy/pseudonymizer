"""
Python Packaging with setuptools
"""
from setuptools import find_packages, setup

setup(
    name="pseudonymizer",
    version="0.0.1",
    author="Victor Pongnian",
    author_email="vpongnian@equancy.com",
    description="""
        Pseudonymization module.
    """,
    keywords="pseudonymization hmac",
    license="Other/Proprietary License",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Information Technology",
        "License :: Other/Proprietary License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 2/3",
        "Topic :: Utilities",
    ],
    install_requires=[
        "Click==7.0",
        "PyYAML==5.4",
        "pandas==0.24.2",
        "SQLAlchemy==1.3.5",
        "xlrd==1.2.0",
        "psycopg2-binary==2.8.3"
    ],
    entry_points={
        "console_scripts": ["pseudonymizer-cli = pseudonymizer.cli:main"]
    },
)