#!/bin/bash

node scraper.js; status=$?;
echo $status
while [ $status -ne 0 ]
do
	node scraper.js; status=$?;
	sleep 1
done
