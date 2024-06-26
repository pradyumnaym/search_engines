#!/bin/bash
python crawl_webpages.py
while [ $? -ne 0 ]; do
    python crawl_webpages.py
done