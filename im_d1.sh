#!/bin/bash

wkhtmltopdf -q $2 --load-error-handling ignore --page-width 99.219 --page-height 200 --viewport-size 375x1024 --custom-header "User-Agent" "Mozilla/5.0 (iPhone; CPU iPhone OS 9_0 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13A340 Safari/601.1" --custom-header-propagation -q --image-quality 85 --margin-top 15 --margin-bottom 0 --margin-left 0 --margin-right 0  $1 -;echo $1  >>files_mi.txt
