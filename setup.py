""" Setup file """
import os
import re
from setuptools import setup, find_packages

HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, 'README.rst')) as readme_file:
    README = readme_file.read()
with open(os.path.join(HERE, 'CHANGES.rst')) as changes_file:
    CHANGES = changes_file.read()
# Remove custom RST extensions for pypi
CHANGES = re.sub(r'\(\s*:(issue|pr|sha):.*?\)', '', CHANGES)

REQUIREMENTS = [
    'sqlalchemy'
]

TEST_REQUIREMENTS = [
    'pytest'
]

if __name__ == "__main__":
    setup(
        name='jsonquery',
        version='1.0.1',
        description="Basic json -> sqlalchemy query builder",
        long_description=README + '\n\n' + CHANGES,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.2',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Topic :: Software Development :: Libraries',
            'Topic :: Software Development :: Libraries :: Python Modules'
        ],
        author='Joe Cross',
        author_email='joe.mcross@gmail.com',
        url='http://jsonquery.readthedocs.org/',
        license='MIT',
        keywords='json sqlalchemy sql orm',
        platforms='any',
        include_package_data=True,
        py_modules=['jsonquery'],
        packages=find_packages(exclude=('tests',)),
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + TEST_REQUIREMENTS,
    )
