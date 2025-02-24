import setuptools

module = '{{ cookiecutter.project_name }}'

with open("version", "r", encoding="utf-8") as fh:
    version = fh.read()

with open("README.md", "r", encoding="utf-8") as fh:
    description = fh.read()

with open(f"src/{module}/version.py", "w") as f:
    f.write(f'__version__ = "{version}"')

setuptools.setup(
    name=module,
    version=version,
    author="{{ cookiecutter.author }}",
    email="{{ cookiecutter.author_email }}",
    description="{{ cookiecutter.short_description }}",
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
    install_requires=[]
)