import pathlib

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(pathlib.Path(__file__).parent / "datajoint_utilities" / "version.py") as f:
    exec(f.read())

with open(pathlib.Path(__file__).parent / "requirements.txt") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="datajoint_utilities",
    version=__version__,
    author="Thinh Nguyen",
    author_email="thinh@datajoint.com",
    description="Unofficial utilities to support the DataJoint framework.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datajoint-company/datajoint-utilities",
    packages=setuptools.find_packages(exclude=["test*", "docs"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": ("tmplcfg=datajoint_utilities.cmdline.tmplcfg:cli",),
    },
    install_requires=requirements,
    zip_safe=False,
)
