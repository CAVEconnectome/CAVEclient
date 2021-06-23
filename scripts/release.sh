#!/bin/bash
bumpversion $1
git push && git push --tags
TAG=$(git describe --tags)
python setup.py sdist
twine upload dist/caveclient-${TAG:1}.tar.gz
