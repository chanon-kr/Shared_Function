import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Shared_Function",
    version="0.0.8",
    author="Chanon Krittapholchai",
    author_email="chanon.krittapholchai@gmail.com",
    description="A shared function that use within my team",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chanon-kr/Shared_Function",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 1 - Planning", "Programming Language :: Python :: 3"
    ]
)