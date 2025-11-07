# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

import os

from setuptools import setup, find_namespace_packages


BASEDIR = os.path.abspath(os.path.dirname(__file__))


def find_version():
    verfile = os.path.join(BASEDIR, 'xflow', 'framework', 'version.py')
    with open(verfile, 'r', encoding='utf8') as f:
        for line in f.readlines():
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip("'").strip('"')


def find_requires():
    reqfile = os.path.join(BASEDIR, 'requirements.txt')
    with open(reqfile, 'r', encoding='utf8') as f:
        return [r.strip() for r in f.readlines()]
    

def find_long_description():
    desc = ''
    for readme in ('README.md'):
        if desc:
            desc += '\n***\n\n'
        with open(readme, encoding='utf8') as f:
            desc += ''.join(f.readlines()[6:])
    desc = desc.replace('/blob/master/',
                        f'/blob/v{find_version()}/')
    return desc


setup(
    name='xflow.framework',
    version=find_version(),
    description='可以用 Python 写 Pipeline 的 CICD 框架。',
    long_description=find_long_description(),
    long_description_content_type='text/markdown',
    author='zhaowcheng',
    author_email='zhaowcheng@163.com',
    install_requires=find_requires(),
    packages=find_namespace_packages(),
    entry_points={
        'console_scripts': [
            'xflow = xflow.framework.main:main'
        ]
    },
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12"
    ],
    url='https://github.com/zhaowcheng/xflow.framework',
    python_requires='>=3.6',
    project_urls={
        'Homepage': 'https://github.com/zhaowcheng/xflow.framework',
        'Issues': 'https://github.com/zhaowcheng/xflow.framework/issues'
    }
)
