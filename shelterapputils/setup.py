import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="shelterapputils",  # Replace with your own username
    version="0.0.1",
    author="Hunter Kuffel",
    author_email="shelterappinfo@gmail.com",
    description="Some utilities regarding locating potential duplicates for an app about" \
                "emergency facilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ShelterApp/AddResources",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pymongo',
        're',
        'tqdm',
        'collections'
    ]
)
