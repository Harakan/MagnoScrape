#!/bin/bash

#/home/Red/Projects/MagnoScrape/timelapse/genconfig.py '/home/Red/Projects/MagnoScrape/pictures/*.png' > Weather.json
#/home/Red/Projects/MagnoScrape/timelapse/timelapse.py Weather.json


avconv -r 5 -i "/home/Red/Projects/MagnoScrape/shortLapse/%04d_market.png" -crf 20 -q 15 magno.mp4

