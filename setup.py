#!/usr/bin/env python
from distutils.core import setup

setup(name='pipeline',
      version='1.0',
      description='a tiny yet another library for chaining unit of work(job) and executing in parallel',
      author='Taejun Colin Jang',
      author_email='kr50cc@gmail.com',
      url='https://www.python.org/sigs/distutils-sig/',
      package_dir={'': 'src'},
      py_modules=['pipeline', 'counter'])
