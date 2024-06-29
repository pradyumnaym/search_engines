#!/bin/bash
python clean_frontier.py
python crawl_webpages.py
while [ $? -ne 0 ]; do
    python clean_frontier.py
    python crawl_webpages.py
done
