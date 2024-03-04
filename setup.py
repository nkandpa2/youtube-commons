from setuptools import setup, find_packages


# Read the contents of requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()


setup(
    name='youtube-commons',
    version='1.0.0',
    packages=find_packages(),
    # Include additional files like non-code resources or configuration files
    include_package_data=True,
    # Provide a short description of your package
    description='A package for cataloging Creative Commons videos posted to YouTube',
    # Add your author information
    author='Nikhil Kandpal',
    author_email='nkandpa2@gmail.com',
    # Add project URL if any
    url='https://github.com/nkandpa2/youtube-commons',
    # Add any required dependencies
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'cc-videos = yt_commons.scripts.cc_videos:main',
        ],
    },
    # Add classifiers to specify the audience and maturity of your package
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)

