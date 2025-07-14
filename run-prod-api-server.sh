#!/bin/bash

cp server/app.py deployed_app.py
mkdir -p /tmp/gunicorn
gunicorn --worker-tmp-dir /tmp/gunicorn --workers=4 deployed_app:app -b 0.0.0.0:5001
