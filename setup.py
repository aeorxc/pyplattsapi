import setuptools

setuptools.setup(
    name="plattsapi",
    version="0.0.1",
    author="aeorxc",
    description="Wrapper around Platts API",
    url="https://github.com/aeorxc/plattsapi",
    project_urls={
        "Source": "https://github.com/aeorxc/plattsapi",
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["pandas", "requests"],
    python_requires=">=3.8",
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)