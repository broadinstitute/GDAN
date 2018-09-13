import os
from setuptools import setup, find_packages
_README           = os.path.join(os.path.dirname(__file__), 'README.md')
_LONG_DESCRIPTION = open(_README).read()

# Setup information
setup(
    name = 'gdan',
    packages = find_packages(),
    description = 'GDAN: A collection of tools for running GDAN analyses in FireCloud',
    author = 'Broad Institute Processing Genome Data Analysis Center',
    author_email = 'gdac@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    license = "BSD 3-Clause License",
    url = 'https://github.com/broadinstitute/HydrantFC',
    entry_points = {
        'console_scripts': [
            'analyses_new = gdan.analyses_new:main',
            'stddata_new = gdan.stddata_new:main',
            'gdac_new = gdan.gdac_new:main'
        ]
    },
    package_data = {'gdan': ['defaults/*']},
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    install_requires = [
        'firecloud>=0.16.14'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
    ]
)
