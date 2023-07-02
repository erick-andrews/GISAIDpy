from setuptools import setup

setup(
    name='GISAIDpy',
    version='0.0.1',
    author='Erick Andrews',
    author_email='andrews_erick@proton.me',
    description='Python port of Wytamma GISAIDR package in R',
    url='https://github.com/erick-andrews/GISAIDpy',
    packages=['GISAIDpy'],
    install_requires=[
        'numpy',
        'polars',
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
