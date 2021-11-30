import setuptools
import pathlib

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(pathlib.Path(__file__).parent / 'dj_search' / 'meta.py') as f:
    exec(f.read())

with open(pathlib.Path(__file__).parent / 'requirements.txt') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name=pkg_name,
    version=__version__,
    author="Thinh Nguyen",
    author_email="thinh@vathes.com",
    description="Unofficial search utility for DataJoint pipeline.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datajoint/dj-search",
    packages=setuptools.find_packages(exclude=['test*', 'docs']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=requirements
)