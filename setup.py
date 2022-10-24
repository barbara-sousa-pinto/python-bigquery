
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

dependencies = [
    "google-cloud-bigquery",
]

test_requirements = [
    "mock",
    "pytest",
]

# TODO: setup
setuptools.setup(
    name="python_bigquery",
    version="0.0.1",
    # author="Example Author",
    # author_email="author@example.com",
    description="A package to make easy work with 'google-cloud-bigquery' in data pipelines.",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    # url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=dependencies,
    tests_require=test_requirements,
)