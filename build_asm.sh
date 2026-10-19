#!/bin/sh
# Builds libvera_asm.so from vera_asm.S (x86-64 Linux). Needs gcc:  sudo apt install build-essential
set -e
cd "$(dirname "$0")"
gcc -shared -nostdlib -o libvera_asm.so vera_asm.S
echo "built libvera_asm.so"
# test with the bot's own Python (.venv has numpy/pandas), else the system one
if [ -x .venv/bin/python ]; then .venv/bin/python vera_asm.py; else python3 vera_asm.py; fi
