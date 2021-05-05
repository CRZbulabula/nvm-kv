#include <assert.h>
#include <stdio.h>

#include <string>
#include <vector>

#include "include/engine.h"
#include "test_util.h"

using namespace polar_race;

#define KV_CNT 100000
#define Q_CNT 100

class rangeVistor : public Visitor {
	public:
		std::vector<PolarString> keyList, valueList;
		void Visit(const PolarString& key, const PolarString& value) {
			keyList.push_back(key);
			valueList.push_back(value);
		}
		void clear() {
			keyList.clear();
			valueList.clear();
		}
		int size() {
			return keyList.size();
		}
};

char k[8192];
char v[9024];
std::string ks[KV_CNT];
std::string vs_1[KV_CNT];
std::string vs_2[KV_CNT];
std::string vs_3[KV_CNT];
int main() {
	Engine *engine = NULL;
	printf_(
		"======================= single range query test "
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
		sprintf(k, "%06d", i);
		ks[i] = k;
		gen_random(v, 32);
		vs_1[i] = v;
		vs_3[i] = v;
	}

	printf("gen OK\n");
	//printf("%s %s\n", vs_1[0].c_str(), vs_3[0].c_str());

	for (int i = 0; i < KV_CNT; ++i) {
		ret = engine->Write(ks[i], vs_1[i]);
		gen_random(v, 32);
		vs_1[i] = v;
		assert(ret == kSucc);
	}
	
	printf("write OK\n");

	rangeVistor visitor;
	ret = engine->Range("", "", visitor);
	assert(ret == kSucc);
	assert(visitor.size() != KV_CNT);
	for (int i = 0; i < KV_CNT; ++i) {
		//printf("%s %s\n", ks[i].c_str(), visitor.keyList[i].data());
		//printf("%s %s\n", vs_3[i].c_str(), visitor.valueList[i].data());
		assert(ks[i] == visitor.keyList[i]);
		assert(vs_3[i] == visitor.valueList[i]);
	}
	printf("full ok\n");

	for (int i = 0; i < Q_CNT; ++i) {
		int lower = rand() % KV_CNT;
		int upper = rand() % KV_CNT;
		if (lower > upper) {
			std::swap(lower, upper);
		}

		visitor.clear();
		ret = engine->Range(ks[lower], ks[upper], visitor);
		assert(ret == kSucc);
		for (int i = 0; i < upper - lower; i++) {
			assert(ks[i + lower] == visitor.keyList[i]);
			assert(vs_3[i + lower] == visitor.valueList[i]);
		}
	}
	
	printf("random range query OK\n");

	delete engine;

	printf_(
		"======================= single range query test pass :) "
		"======================");

	return 0;
}
