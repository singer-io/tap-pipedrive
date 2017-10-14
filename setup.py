from setuptools import setup, find_packages


setup(
    name="tap-pipedrive",
    version="0.0.2",
    license="AGPL",
    description="Singer.io tap for extracting data from the Pipedrive API",
    author="Stitch",
    author_email="dev@stitchdata.com",
    url="https://singer.io",
    py_modules=['tap_pipedrive'],
    packages=find_packages(),
    install_requires=[
        'singer-python==3.6.3',
        'requests==2.12.4',
    ],
    entry_points={
        'console_scripts': [
            'tap-pipedrive = tap_pipedrive.cli:main',
        ]
    },
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development :: Pre-processors",
    ],
)
