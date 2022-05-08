from setuptools import setup

microlib_name = "flaightkitplugins-pod"

plugin_requires = [
    "kubernetes>=12.0.1",
]

__version__ = "0.1.0"

setup(
    name=microlib_name,
    version=__version__,
    author="flyteorg",
    author_email="admin@flyte.org",
    description="Flytekit plugin to support K8s Pod tasks",
    namespace_packages=["flytekitplugins"],
    packages=["flytekitplugins.pod"],
    install_requires=plugin_requires,
    license="apache2",
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
