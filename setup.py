from setuptools import setup, find_packages

setup(
    name="easysql",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "pandas>=1.3.0",
        "pydantic>=2.0.0",
        "sqlalchemy>=1.4.0",
        "python-multipart>=0.0.5",
        "pytest>=6.0.0",
        "httpx>=0.18.0",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "black",
            "isort",
            "mypy",
            "flake8",
        ]
    },
    python_requires=">=3.9",
    author="Your Name",
    author_email="your.email@example.com",
    description="A simple SQL database management tool",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/easysql",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
    ],
)