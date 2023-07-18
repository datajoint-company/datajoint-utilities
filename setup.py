import pathlib

import setuptools

pkg_name = "datajoint_utilities"

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(pathlib.Path(__file__).parent / pkg_name / "version.py") as f:
    exec(f.read())

with open(pathlib.Path(__file__).parent / "requirements.txt") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name=pkg_name.replace("_", "-"),
    version=__version__,
    author="Thinh Nguyen",
    author_email="thinh@datajoint.com",
    description="Unofficial utilities to support the DataJoint framework.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github.com/datajoint-company/{pkg_name.replace('_', '-')}",
    packages=setuptools.find_packages(exclude=["contrib", "docs", "tests*"]),
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
