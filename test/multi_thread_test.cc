#include <assert.h>
#include <stdio.h>

#include <string>
#include <thread>

#include "include/engine.h"
#include "test_util.h"
#include <time.h>


using namespace polar_race;

#define KV_CNT 1000
#define THREAD_NUM 4
#define CONFLICT_KEY 50

char k[1024];
char v[9024];
std::string ks[THREAD_NUM][KV_CNT];
std::string vs[THREAD_NUM][KV_CNT];
Engine *engine = NULL;

void test_thread(int id) {
    RetCode ret;
    std::string value;
    for (int i = 0; i < KV_CNT; ++i) {
        ret = engine->Write(ks[id][i], vs[id][i]);
        assert(ret == kSucc);

        ret = engine->Read(ks[id][i], &value);
        assert(ret == kSucc);
        assert(value == vs[id][i]);
    }
}

void test_thread_conflict(int id) {
    RetCode ret;
    std::string value;

    for (int k = 0; k < 10000; ++k) {
        //printf("%d\n",k);
        for (int i = 0; i < CONFLICT_KEY; ++i) {
            ret = engine->Write(ks[0][i], vs[id][i]);
            assert(ret == kSucc);
        }
    }
   // printf("77");
}

int main() {
    printf_(
        "======================= multi thread test "
        "============================");
#ifdef MOCK_NVM
    std::string engine_path =
        std::string("/tmp/ramdisk/data/test-") + std::to_string(asm_rdtsc());
#else
    std::string engine_path = "/dev/dax0.0";
#endif
    RetCode ret = Engine::Open(engine_path, &engine);
    assert(ret == kSucc);
    printf("open engine_path: %s\n", engine_path.c_str());

    clock_t start_t,end_t;
    start_t = clock();

    for (int t = 0; t < THREAD_NUM; ++t) {
        for (int i = 0; i < KV_CNT; ++i) {
            gen_random(k, 6);
            ks[t][i] =
                std::to_string(t) + "th" + std::string(k) + std::to_string(i);

            gen_random(v, 1027);
            vs[t][i] = v;
        }
    }

    std::thread ths[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; ++i) {
        ths[i] = std::thread(test_thread, i);
    }
    for (int i = 0; i < THREAD_NUM; ++i) {
        ths[i].join();
    }
    //printf("11111111\n");
    std::string value;
    for (int t = 0; t < THREAD_NUM; ++t) {
        for (int i = 0; i < KV_CNT; ++i) {
            ret = engine->Read(ks[t][i], &value);
            assert(ret == kSucc);
            assert(value == vs[t][i]);
        }
    }
    //printf("222222\n");
    ////////////////////////////////////////////////////////////////////
    for (int i = 0; i < THREAD_NUM; ++i) {
        ths[i] = std::thread(test_thread_conflict, i);
    }
    //printf("333333\n");
    for (int i = 0; i < THREAD_NUM; ++i) {
       // printf("66\n");
        ths[i].join();
    }
    for (int i = 0; i < CONFLICT_KEY; ++i) {
        ret = engine->Read(ks[0][i], &value);
        assert(ret == kSucc);
        bool found = false;
        for (int t = 0; t < THREAD_NUM; ++t) {
            if (value == vs[t][i]) {
                found = true;
                break;
            }
        }
        assert(found);
    }

    delete engine;

    end_t = clock();

    printf("time is:%f\n",(double)(end_t-start_t)/CLOCKS_PER_SEC);

    printf_(
        "======================= multi thread test pass :) "
        "======================");

    return 0;
}
