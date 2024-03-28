import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="clickhouse-query",
    version="0.0.7",
    author="Mehrshad Hosseini",
    author_email="mehrh8@gmail.com",
    description="clickhouse query",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mehrh8/clickhouse-query",
    project_urls={
        "Bug Tracker": "https://github.com/mehrh8/clickhouse-query/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(".", exclude=["tests*"]),
    python_requires=">=3.6",
)
