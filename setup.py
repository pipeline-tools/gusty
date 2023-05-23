import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gusty",
    version="0.21.0",
    author="Chris Cardillo, Michael Chow, David Robinson",
    author_email="cfcardillo23@gmail.com",
    description="Making DAG construction easier",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pipeline-tools/gusty",
    packages=setuptools.find_packages(),
    install_requires=[
        "ABSQL",
        "apache-airflow",
        "click",
        "inflection",
        "jupytext",
        "nbformat",
        "pendulum",
        "PyYaml",
    ],
    entry_points={
        "console_scripts": [
            "gusty = gusty.cli:cli",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
