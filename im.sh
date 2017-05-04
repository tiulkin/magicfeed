#!/bin/bash

wkhtmltopdf  $3 --page-width $2 --page-height 300 --minimum-font-size 20 --viewport-size 1024x1024 -q --image-quality 85 --margin-top 15 --margin-bottom 0 --margin-left 0 --margin-right 0 --dpi 72 $1 -;echo $1 $3 >>files_m.txt