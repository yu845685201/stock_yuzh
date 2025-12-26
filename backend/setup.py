"""
A股盘后静态分析系统安装脚本
"""

from setuptools import setup, find_packages

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="stock-analysis-system",
    version="0.1.0",
    description="A股盘后静态分析系统",
    author="yuzh",
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'stock-cli=src.cli.main:main',
        ],
    },
    python_requires='>=3.8',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)