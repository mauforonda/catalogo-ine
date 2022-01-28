#!/usr/bin/env bash

jupyter nbconvert --to notebook --inplace --execute index.ipynb
./update/publish.sh index.ipynb
