# Non-volatile Memory Based Key Value Store

## All you have to do

Complete engine_race/engine_race.[h,cc], and execute

```
make
```
to build your own engine

It is equivalent to `make MOCK_NVM=1` and will use mock NVM device by default.

## Example for you

For quick start, we have already implemented a simple
example engine in engine_example, you can view it and execute

```
make TARGET_ENGINE=engine_example
```
to build this example engine

## Correctness Test

After building the engine (`make` for your implementation, or `make TARGET_ENGINE=engine_example` for the example)

```
cd test
./build.sh
./run_test.sh
```

## Performance Test

After building the engine

```
cd bench
./build.sh

# type ./bench to see the usage
./bench 1 100 0
```

## Run with Real NVM
```
make MOCK_NVM=0

cd bench
./build-real-nvm.sh

cd test
./build-real-nvm.sh
```