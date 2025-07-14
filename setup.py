from setuptools import setup


setup(name="tap-pipedrive",
      version="1.3.1",
      description="Singer.io tap for extracting data from the Pipedrive API",
      author="Stitch",
      author_email="dev@stitchdata.com",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_pipedrive"],
      install_requires=[
          "pendulum==3.1.0",
          "requests==2.32.4",
          "singer-python==6.1.1",
      ],
      entry_points="""
          [console_scripts]
          tap-pipedrive=tap_pipedrive.cli:main
      """,
      packages=["tap_pipedrive",
                "tap_pipedrive.streams"],
      include_package_data=True,
)
