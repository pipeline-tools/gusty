import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gusty",
    version="0.0.5",
    author="Chris Cardillo",
    author_email="cfcardillo23@gmail.com",
    description="An opinionated framework for ETL built on top of Airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chriscardillo/gusty",
    packages=setuptools.find_packages(),
    install_requires=[
          'apache-airflow',
          'jinja2',
          'inflection',
          'nbformat',
          'python-frontmatter',
          'sshtunnel',
          'pysftp'
    ],
    entry_points = {
        'airflow.plugins': [
        #    'gusty = gusty.gusty_plugin:GustyPlugin'
        ]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
