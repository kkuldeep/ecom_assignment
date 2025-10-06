"""
Setup script for E-commerce Data Analytics Pipeline
Allows installation as a Python package
"""

from setuptools import setup, find_packages
import os

# Read README for long description
def read_readme():
    try:
        with open("README.md", "r", encoding="utf-8") as fh:
            return fh.read()
    except FileNotFoundError:
        return "E-commerce Data Analytics Pipeline - A PySpark-based data processing pipeline"

# Read requirements
def read_requirements():
    try:
        with open("requirements.txt", "r", encoding="utf-8") as fh:
            return [line.strip() for line in fh if line.strip() and not line.startswith("#")]
    except FileNotFoundError:
        return [
            "pyspark>=3.5.0",
            "pandas>=1.5.0", 
            "pytest>=7.0.0",
            "openpyxl>=3.0.0"
        ]

setup(
    name="ecommerce-analytics-pipeline",
    version="1.0.0",
    author="Kuldeep Kumar",
    author_email="kuldeep.iiest@gmail.com",
    description="A PySpark-based data processing pipeline for e-commerce analytics",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/kkuldeep/ecom_assignment",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.7",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "jupyter>=1.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0"
        ],
        "notebooks": [
            "jupyter>=1.0.0",
            "matplotlib>=3.5.0",
            "seaborn>=0.11.0"
        ]
    },
    entry_points={
        "console_scripts": [
            "ecommerce-test=run_tests:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt", "*.yml", "*.yaml"],
        "data": ["*.csv", "*.json", "*.xlsx"],
    },
    zip_safe=False,
    keywords="pyspark, data-analytics, e-commerce, etl, data-pipeline, test-driven-development",
    project_urls={
        "Bug Reports": "https://github.com/kkuldeep/ecom_assignment/issues",
        "Source": "https://github.com/kkuldeep/ecom_assignment",
        "Documentation": "https://github.com/kkuldeep/ecom_assignment/blob/main/README.md",
    },
)