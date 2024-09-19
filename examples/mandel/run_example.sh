#!/usr/bin/env bash

reset

export PYTHONPATH=$PYTHONPATH:"../../src/"

python3 main.py --server &
sleep 2
python3 main.py &
#sleep 2
#python3 main.py &
#sleep 2
#python3 main.py &
#sleep 2
#python3 main.py &

