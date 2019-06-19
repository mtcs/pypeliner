import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pypeliner-workflow",
    version="0.1",
    author="Matheus Caldas Santos",
    author_email="matheuscs@gmail.com",
    description="Organize your data workflow with pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mtcs/pypeliner-workflow",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
