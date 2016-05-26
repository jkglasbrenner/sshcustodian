# File: setup.py
# -*- coding: utf-8 -*-

# Import modules
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Create setup information
setup(
    name='SSHCustodian',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['custodian', 'six', 'monty'],
    version='0.2.5',
    description=('A modification to the Custodian class in custodian '
                 '(github.com/materialsproject/custodian) to allow for '
                 'copying the temp_dir to other compute nodes via ssh.'),
    long_description=long_description,
    author='James K. Glasbrenner',
    author_email='jkglasbrenner@gmail.com',
    url='https://github.com/jkglasbrenner/sshcustodian',
    download_url='https://github.com/jkglasbrenner/sshcustodian/tarball/0.2.5',
    license='MIT',
    keywords='custodian DFT VASP materials hpc queue management',
    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5'
    ]
)
