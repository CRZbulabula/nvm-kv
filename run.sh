make clean && make
cd bench
./build.sh
./bench 1 0 0
./bench 1 50 0
./bench 1 100 0
cd ../test
./build.sh
./run_test.sh