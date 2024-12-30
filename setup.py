from setuptools import find_packages, setup

setup(
    name="kdags",
    packages=find_packages(exclude=["kdags_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        # "boto3",
        "pandas",
        "matplotlib",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
