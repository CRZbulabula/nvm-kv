make clean && make
cd test
./build.sh
./run_test.sh
cd ../bench
./build.sh
./bench 1 0 0
./bench 1 50 1
./bench 1 100 1