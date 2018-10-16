from setuptools import setup


setup(name="tap-pipedrive",
      version="0.1.1",
      description="Singer.io tap for extracting data from the Pipedrive API",
      author="Stitch",
      author_email="dev@stitchdata.com",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_pipedrive"],
      install_requires=[
          "singer-python==3.6.3",
          "requests==2.12.4",
      ],
      entry_points="""
          [console_scripts]
          tap-pipedrive=tap_pipedrive.cli:main
      """,
      packages=["tap_pipedrive",
                "tap_pipedrive.streams",
                "tap_pipedrive.streams.recents",
                "tap_pipedrive.streams.recents.dynamic_typing"],
      include_package_data=True,
)
