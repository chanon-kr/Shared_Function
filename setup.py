import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = [
#                "pymssql == 2.1.4",
#                "Office365-REST-Python-Client"
#                ,"SQLAlchemy"
#                ,"pandas >= 1" , "requests"
              ]

setuptools.setup(
    name="py_topping",
    version="0.3.5",
    license = "MIT",	
    author="Chanon Krittapholchai",
    author_email="chanon.krittapholchai@gmail.com",
    description="simplify functions from other libraries functions in 1-2 lines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chanon-kr/Shared_Function",
    download_url = "https://github.com/chanon-kr/Shared_Function/archive/0.3.4.tar.gz",
    keywords = ["utility","ETL"],
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    install_requires= requirements ,
    python_requires = '>=3.7'
)