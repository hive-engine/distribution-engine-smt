#/bin/bash

cd /home/ubuntu/distribution-engine-smt
cp server/app.py deployed_app.py
gunicorn --worker-tmp-dir /mem --workers=4 deployed_app:app -b 0.0.0.0:5001
