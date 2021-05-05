#include <assert.h>
#include <stdio.h>

#include <string>

#include "include/engine.h"
#include "test_util.h"

using namespace polar_race;

#define KV_CNT 10000

char k[8192];
char v[9024];
std::string ks[KV_CNT];
std::string vs_1[KV_CNT];
std::string vs_2[KV_CNT];
std::string vs_3[KV_CNT];
int main() {
	Engine *engine = NULL;
	printf_(
		"======================= single middle io test "
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

	///////////////////////////////////
	for (int i = 0; i < KV_CNT; ++i) {
		gen_random(k, 16);
		ks[i] = std::string(k) + std::to_string(i);
		gen_random(v, 32);
		vs_1[i] = v;
		vs_3[i] = v;
		gen_random(v, 32);
		vs_2[i] = v;
	}

	printf("gen OK\n");
	//printf("%s %s\n", vs_1[0].c_str(), vs_3[0].c_str());

	for (int i = 0; i < KV_CNT; ++i) {
		ret = engine->Write(ks[i], vs_1[i]);
		gen_random(v, 16);
		vs_1[i] = v;
		assert(ret == kSucc);
	}
	
	printf("write OK\n");
	//printf("%s %s\n", vs_1[0].c_str(), vs_3[0].c_str());

    std::string value;
	for (int i = 0; i < KV_CNT; ++i) {
		ret = engine->Read(ks[i], &value);
		assert(ret == kSucc);
		assert(value == vs_3[i]);
	}
	
    printf("read OK\n");

	for (int i = 0; i < KV_CNT; ++i) {
		if (i % 2 == 0) {
			ret = engine->Write(ks[i], vs_2[i]);
			assert(ret == kSucc);
		}
	}

	for (int i = 0; i < KV_CNT; ++i) {
		ret = engine->Read(ks[i], &value);
		assert(ret == kSucc);

		if (i % 2 == 0) {
			assert(value == vs_2[i]);
		} else {
			assert(value == vs_3[i]);
		}
	}

    printf("repeat io OK\n");

	delete engine;

	// re-open
	ret = Engine::Open(engine_path, &engine);
	assert(ret == kSucc);
	for (int i = 0; i < KV_CNT; ++i) {
		ret = engine->Read(ks[i], &value);
		assert(ret == kSucc);

		if (i % 2 == 0) {
			assert(value == vs_2[i]);
		} else {
			assert(value == vs_3[i]);
		}
	}

	printf("reopen OK\n");

	printf_(
		"======================= single middle io test pass :) "
		"======================");

	return 0;
}
