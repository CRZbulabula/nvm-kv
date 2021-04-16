// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <pthread.h>
#include <string>
#include <unistd.h>

#include "include/engine.h"
#include "BPlusTree.h"
#include "util.h"

namespace polar_race {

class EngineRace : public Engine {
public:
	static RetCode Open(const std::string& name, Engine** eptr);

	explicit EngineRace(const std::string& dir): 
		mu_(PTHREAD_MUTEX_INITIALIZER),
		db_lock_(NULL) {}

	~EngineRace();

	RetCode Write(const PolarString& key, const PolarString& value) override;

	RetCode Read(const PolarString& key, std::string* value) override;

	/*
	 * NOTICE: Implement 'Range' in quarter-final,
	 *         you can skip it in preliminary.
	 */
	RetCode Range(const PolarString& lower, const PolarString& upper,
				  Visitor& visitor) override;

private:
	pthread_mutex_t mu_;
	FileLock* db_lock_;
	b_plus_tree::bplus_tree store;
};

inline bool operator<(const PolarString& x, const PolarString& y) {
	return x.compare(y) < 0;
}

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
