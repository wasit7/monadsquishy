from setuptools import setup, find_packages

setup(
    name="monadsquishy",
    version="0.1.0",
    description="A Python package for squishy data transformation and monads",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Wasit Limprasert",
    author_email="wasit7@gmail.com",
    url="https://github.com/wasit7/squishy",
    packages=find_packages(),
    install_requires=[
        "pandas==2.1.4"  # Adding pandas as a dependency
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)