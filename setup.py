from setuptools import setup


setup(name="tap-pipedrive",
      version="2.1.0",
      description="Singer.io tap for extracting data from the Pipedrive API",
      author="Stitch",
      author_email="dev@stitchdata.com",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_pipedrive"],
      install_requires=[
<<<<<<< HEAD
          "pendulum==3.2.0",
          "requests==2.33.1",
          "singer-python==6.8.0",
=======
          "pendulum==3.1.0",
          "requests==2.33.0",
          "singer-python==6.1.1",
>>>>>>> origin/master
      ],
      entry_points="""
          [console_scripts]
          tap-pipedrive=tap_pipedrive.cli:main
      """,
      packages=["tap_pipedrive",
                "tap_pipedrive.streams"],
      include_package_data=True,
)
