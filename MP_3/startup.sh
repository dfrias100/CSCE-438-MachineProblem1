#!/bin/bash

./coordinator -p 9000 &

./tsd -c localhost -d 9000 -p 6510 -i 1 -t slave &
./tsd -c localhost -d 9000 -p 6520 -i 2 -t slave &
./tsd -c localhost -d 9000 -p 6530 -i 3 -t slave &

sleep 5

./tsd -c localhost -d 9000 -p 6610 -i 1 -t master &
./tsd -c localhost -d 9000 -p 6620 -i 2 -t master &
./tsd -c localhost -d 9000 -p 6630 -i 3 -t master &

./synchronizer -c localhost -d 9000 -p 6710 -i 1 &
./synchronizer -c localhost -d 9000 -p 6720 -i 2 &
./synchronizer -c localhost -d 9000 -p 6730 -i 3 &