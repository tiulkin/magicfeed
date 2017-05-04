#!/bin/bash

wkhtmltopdf  $3 --page-width 270 --page-height 200 --viewport-size 1024x1024 --minimum-font-size 14 -q --image-quality 85 --margin-top 15 --margin-bottom 0 --margin-left 0 --margin-right 0 --dpi 72 $1 -;
