import os
from setuptools import setup, find_packages

README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()
LICENSE = open(os.path.join(os.path.dirname(__file__), 'LICENSE')).read()


with open('requirements.txt') as f:
    required = f.read().splitlines()


setup(
    name='alphabot',
    version=open("alphabot/_version.py").readlines()[-1].split()[-1].strip("\"'"),
    python_requires='>=3.5.2',
    install_requires=required,
    include_package_data=True,
    license=LICENSE, #or 'BSD License'
    description='Algorithmic based automatic trader.',
    long_description=README,
    url='https://github.com/crystalphi/alphabot',
    author='crystalphi.z',
    author_email='crystalphi@gmail.com',
    #packages=['alphabot'],
    packages=find_packages(exclude=('trade_agent', 'tests', 'docs')),
    entry_points={
        'console_scripts': [
            'alphabot = alphabot.cli:main',
        ]
    }
)
