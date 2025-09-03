from pathlib import Path

from setuptools import find_packages, setup

install_requires = [
    "aiobotocore>=2.24.1",
    'typing-extensions>=4; python_version<"3.11"',
]

setup(
    name="as-aiopynamodb",
    version=__import__("aiopynamodb").__version__,
    packages=find_packages(
        exclude=(
            "examples",
            "tests",
            "typing_tests",
            "tests.integration",
        )
    ),
    url="http://jlafon.io/pynamodb.html",
    project_urls={
        "Fork": "https://github.com/AppSolves/AioPynamoDB",
        "Original": "https://github.com/brunobelloni/AioPynamoDB",
    },
    author="Jharrod LaFon",
    author_email="jlafon@eyesopen.com",
    description="An Async Pythonic Interface to DynamoDB",
    long_description=Path("README.rst").read_text("utf-8"),
    long_description_content_type="text/x-rst",
    zip_safe=False,
    license="MIT",
    keywords="python dynamodb amazon async asyncio aiobotocore",
    python_requires=">=3.10",
    install_requires=install_requires,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
    ],
    extras_require={
        "signals": ["blinker>=1.3,<2.0"],
    },
    package_data={"aiopynamodb": ["py.typed"]},
)
