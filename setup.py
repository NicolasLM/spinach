from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'LICENSE'), encoding='utf-8') as f:
    long_description += f.read()

with open(path.join(here, 'spinach', 'const.py'), encoding='utf-8') as fp:
    version = dict()
    exec(fp.read(), version)
    version = version['VERSION']

setup(
    name='spinach',
    version=version,
    description='Modern Redis task queue for Python 3',
    long_description=long_description,
    url='https://github.com/NicolasLM/spinach',
    author='Nicolas Le Manchet',
    author_email='nicolas@lemanchet.fr',
    license='BSD 2-clause',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='task queue jobs redis',

    packages=find_packages(include=('spinach', 'spinach.*')),
    install_requires=[
        'redis',
        'blinker'
    ],

    extras_require={
        'tests': [
            'pytest',
            'pytest-cov',
            'pytest-threadleak',
            'pycodestyle'
        ],
    },

    package_data={
        'spinach.brokers.redis_scripts': [
            'move_future_jobs.lua',
            'enqueue_job.lua',
            'enqueue_future_job.lua',
            'flush.lua',
            'get_jobs_from_queue.lua',
            'register_periodic_tasks.lua'
        ],
    },
)
