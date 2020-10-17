from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="shelterapp_add_resources",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
)
