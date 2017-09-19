#!/bin/bash

python setup.py sdist && \
    twine upload dist/* -r testpypi