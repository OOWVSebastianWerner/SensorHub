from setuptools import find_packages, setup

setup(
    name="dagster_sh",
    packages=find_packages(exclude=["dagster_sh_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
