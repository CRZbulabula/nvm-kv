make clean && make
cd bench
./build.sh
# ./bench 1 50 0
# ./bench 1 50 1
# ./bench 2 50 0
# ./bench 2 50 1
# ./bench 3 50 0
# ./bench 3 50 1
# ./bench 4 50 0
# ./bench 4 50 1

./bench 1 0 0
./bench 1 100 0
./bench 1 0 1
./bench 1 100 1
cd ../test
./build.sh
./run_test.sh