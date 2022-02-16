#!/usr/bin/env bash

echo -n 'status,' ; head -n 1 catalogo_ine.csv
git diff -U0 catalogo_ine.csv | grep '^[+-]' | grep -Ev '^(--- a/|\+\+\+ b/)' | sed 's/^\+/+,/g' | sed 's/^\-/-,/g'
