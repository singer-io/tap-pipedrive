from setuptools import setup, find_packages
from tap_pipedrive import __version__


setup(
    name="tap-pipedrive",
    version=__version__,
    license="MIT",
    description="Singer.io tap for extracting data from the Pipedrive API",
    author="Stitch",
    author_email="eduard.kracmar@hotovo.org",
    url="https://singer.io",
    py_modules=['tap_pipedrive'],
    packages=find_packages(),
    install_requires=[
        "click>=6.7",
        'attrs==16.3.0',
        'singer-python==3.1.1',
        'requests==2.12.4',
        'backoff==1.3.2',
        'requests_mock==1.3.0',
        'nose'
    ],
    entry_points={
        'console_scripts': [
            'tap-pipedrive = tap_pipedrive.cli:main',
        ]
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development :: Pre-processors",
    ],
)
