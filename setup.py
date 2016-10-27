from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='odincontrib.aws',

    version='0.3.2',

    description='Odin integration with AWS',
    long_description=long_description,

    url='https://github.com/python-odin/odin.contrib-dynamodb',

    author='Tim Savage',
    author_email='tim@savage.company',

    license='BSD',

    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: BSD License',

        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],

    keywords='data-structure validation data-mapping aws dynamodb sqs',

    packages=find_packages(include=('odincontrib_aws*',)),

    install_requires=['odin>=0.10b8', 'six'],
)
