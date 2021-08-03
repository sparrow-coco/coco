#!/bin/bash

# usage:
# 1. install requirements: pip install -r requirements.txt
# 2. command format: python main.py ./configures.txt task source start_date end_date output_directory
# task = output|crawler
# source = facebook|twitter
# start_date end_date output_directory are give when task is assigned as output.
# date format is yyyy-mm-dd and start_date is inclusive and end_date is exclusive

# example:
# python main.py ./configures.txt output facebook 2020-12-10 2020-12-12 /home/lgame/
# the output file is /home/lgame/facebook_2020-12-10_2020-12-12.txt
# note: when use crawl, please update chrome_drive_path in configures.txt

python main.py ./configures.txt output facebook 2020-12-10 2020-12-12 /home/lgame/