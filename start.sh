#!/bin/bash
gunicorn analyss:app --workers 4 --bind 0.0.0.0:$PORT --timeout 120