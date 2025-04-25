""" setup.py """
import textwrap
import setuptools


MODULE = 'casalib'


with open("version", "r", encoding="utf-8") as fh:
    version = fh.read()

with open("README.md", "r", encoding="utf-8") as fh:
    description = fh.read()

with open(f"src/{MODULE}/version.py", "w") as f:
    f.write(textwrap.dedent(f'''
        """ version.py """
        __version__ = "{version}"
    '''))

setuptools.setup(
    name=MODULE,
    version=version,
    author="Paulo Azevedo",
    email="pazevedojr@gmail.com",
    description="A library to automate some activities",
    long_description=description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    package_dir={"": "src"},
    package=setuptools.find_packages(where="src"),
    python_require=">=3.10",
    include_package_data=True,
    install_requires=[
        "awswrangler",
        "beautifulsoup4",
        "boto3",
        "cookiecutter",
        "fire",
        "jinja2",
        "numpy",
        "pandas",
        "requests",
        "pygit2",
    ]
)