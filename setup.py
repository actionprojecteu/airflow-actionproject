import os
import os.path

from setuptools import setup, find_packages, Extension
import versioneer

# Default description in markdown
LONG_DESCRIPTION = open('README.md').read()


PKG_NAME     = 'airflow-actionproject'
AUTHOR       = 'Rafael Gonzalez'
AUTHOR_EMAIL = 'astrorafael@gmail.com'
DESCRIPTION  = 'Airflow components library for ACTION StreetSpectra',
LICENSE      = 'MIT'
KEYWORDS     = 'Astronomy Python CitizenScience LightPollution'
URL          = 'https://github.com/actionprojecteu/airflow-actionproject/'
DEPENDENCIES = [
    "panoptes-client",  # Access to Zooniverse
    "scikit-learn",     # Clustering algorithms
    "folium",           # HTML Maps generation
    "Pillow",           # for maps (image size)
]

CLASSIFIERS  = [
    'Environment :: Console',
    'Intended Audience :: Science/Research',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python :: 3.7',
    'Topic :: Scientific/Engineering :: Astronomy',
    'Topic :: Scientific/Engineering :: Atmospheric Science',
    'Development Status :: 4 - Beta',
]


PACKAGE_DATA = {
    'airflow_actionproject.operators.streetspectra': [
        'templates/*.j2',
    ],
}

SCRIPTS = [
]


DATA_FILES  = [ 

]

setup(
    name             = PKG_NAME,
    version          = versioneer.get_version(),
    cmdclass         = versioneer.get_cmdclass(),
    author           = AUTHOR,
    author_email     = AUTHOR_EMAIL,
    description      = DESCRIPTION,
    long_description_content_type = "text/markdown",
    long_description = LONG_DESCRIPTION,
    license          = LICENSE,
    keywords         = KEYWORDS,
    url              = URL,
    classifiers      = CLASSIFIERS,
    packages         = find_packages("src"),
    package_dir      = {"": "src"},
    install_requires = DEPENDENCIES,
    scripts          = SCRIPTS,
    package_data     = PACKAGE_DATA,
    data_files       = DATA_FILES,
)
