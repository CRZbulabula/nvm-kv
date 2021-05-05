// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "util.h"

namespace polar_race {

static const char kLockFile[] = "LOCK";
static const char kDataFile[] = "DATA";

RetCode Engine::Open(const std::string &name, Engine **eptr) {
	return EngineRace::Open(name, eptr);
}

Engine::~Engine() {}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
	*eptr = NULL;
	EngineRace *engine_race = new EngineRace(name);

	printf("sizeof internal: %d\n", sizeof(b_plus_tree::internalNode));
	printf("sizeof leaf: %d\n", sizeof(b_plus_tree::leafNode));

	// Check dir
	if (opendir(name.c_str()) == NULL && 0 != mkdir(name.c_str(), 0755)) {
		return kIOError;
	}

	// Check data file
	if (!FileExists(name + "/" + kDataFile) && 0 != DataFile(name + "/" + kDataFile)) {
		delete engine_race;
		return kIOError;
	}

	// Check lock file
	//if (!FileExists(name + "/" + kLockFile) && 0 != LockFile(name + "/" + kLockFile, &(engine_race->db_lock_))) {
	//	delete engine_race;
	//	return kIOError;
	//}
	//if (!FileExists(name + "/" + kLockFile) ) {
	//	delete engine_race;
	//	return kIOError;
	//}

	// init B+ tree
	RetCode ret = engine_race->store.init((name + "/" + kDataFile).c_str());
	if (ret != kSucc) {
		delete engine_race;
		return ret;
	}

	*eptr = engine_race;
	return kSucc;
}
 
EngineRace::~EngineRace() {
	//if (db_lock_) {
	//	UnlockFile(db_lock_);
	//}
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
	//pthread_mutex_lock(&mu_);
	RetCode ret = store.insert_or_update(key, value);
	//pthread_mutex_unlock(&mu_);
	return ret;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
	pthread_mutex_lock(&mu_);
	RetCode ret = store.search(key, value);
	pthread_mutex_unlock(&mu_);
	return ret;
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
							 Visitor& visitor) {
	// pthread_mutex_lock(&mu_);
	// std::map<std::string, Location> locations;
	// RetCode ret =
	// 	plate_.GetRangeLocation(lower.ToString(), upper.ToString(), &locations);
	// if (ret != kSucc) {
	// 	pthread_mutex_unlock(&mu_);
	// 	return ret;
	// }

	// std::string value;
	// for (auto& pair : locations) {
	// 	ret = store_.Read(pair.second, &value);
	// 	if (kSucc != ret) {
	// 		break;
	// 	}
	// 	visitor.Visit(pair.first, value);
	// }
	// pthread_mutex_unlock(&mu_);
	// return ret;
	return kSucc;
}

}  // namespace polar_race
