from setuptools import find_packages, setup

setup(
    name="kdags",
    packages=find_packages(exclude=["kdags_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "polars",
        "matplotlib",
        "msal",
        "Office365-REST-Python-Client",
        "openpyxl",
        "plotly",
        "firebase-admin",
        "azure-storage-blob",
        "azure-storage-file-datalake"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
