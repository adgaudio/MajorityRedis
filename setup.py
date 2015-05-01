try:
    from distutils.core import setup  # , Extension
    from setuptools import find_packages
except ImportError:
    print ("Please install Distutils and setuptools"
           " before installing this package")
    raise

setup(
    name='majorityredis',
    version='0.0',
    description=(
        'Distributed consensus algorithms and client-side API to work with N'
        ' independent Redis Servers.'),
    long_description="Check the project homepage for details",
    keywords=[
        'Redis', 'CAP', "Brewer's Theorem", "Majority", "Consensus", "api"],
    author='Alex Gaudio',
    author_email='agaudio@sailthru.com',
    url='https://github.com/adgaudio/majorityredis',

    packages=find_packages(),
    install_requires=[
        'redis>=2.9.1',
        'nose>=1.3.3'
    ],
)