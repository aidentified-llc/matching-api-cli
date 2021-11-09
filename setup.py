from setuptools import setup

with open('requirements.txt', 'r') as fd:
    requirements = [x.strip() for x in fd.readlines()]

setup(
    name='aidentified-matching-api',
    version='0.0.1',
    packages=['aidentified_matching_api'],
    # Let's not force all the hard requirements out from requirements.txt
    # in case people are installing this thing into their system Pythons.
    install_requires=["requirements>=2.26.0"],
    entry_points={
        "console_scripts": ["aidentified_match=aidentified_matching_api:main"]
    }
)
