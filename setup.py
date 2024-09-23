from setuptools import setup, find_packages


setup(
    name="nque",
    version="0.1.1",
    description="Persistent queues",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author="Artur Khakimau",
    author_email="arthur78@gmail.com",
    url="https://github.com/arthur78/nque",
    packages=find_packages(exclude=("tests", "tests.*")),
    classifiers=[
        "Programming Language :: Python :: 3.12",
        # TODO "License :: OSI Approved :: MIT License",
        # TODO "Operating System :: OS Independent",
    ],
    python_requires='>=3.12',
    # TODO platforms="any",
    install_requires=[
        "lmdb",
    ]
    # TODO tests_require=[]
    # TODO extras_require={}
    # TODO cmdclass={"test": PyTest}
)
