from setuptools import setup


setup(
    name='ringmq',
    version='0.1',
    url='http://github.com/mar29th/ring',
    author='Douban Inc.',
    author_email='platform@douban.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: System :: Networking',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: Apache Software License',
    ],
    license="http://www.apache.org/licenses/LICENSE-2.0",
    packages=['ring', 'ring.tests', 'ring.benchmark']
)
