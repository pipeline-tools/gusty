import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gusty",
    version="0.9.1",
    author="Chris Cardillo, Michael Chow, David Robinson",
    author_email="cfcardillo23@gmail.com",
    description="Making DAG construction easier",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chriscardillo/gusty",
    packages=setuptools.find_packages(),
    install_requires=[
        "apache-airflow",
        "inflection",
        "jupytext",
        "nbformat",
        "pendulum",
        "PyYaml",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
