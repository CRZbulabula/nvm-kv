#ifndef _latch_h_
#define _latch_h_

#include <pthread.h>

typedef struct latch
{
	pthread_rwlock_t val[1];
}latch;

#define latch_init(latch)  pthread_rwlock_init((latch)->val, 0);   //初始化读写锁
#define latch_rlock(latch) pthread_rwlock_rdlock((latch)->val);    //读锁定读写锁
#define latch_wlock(latch) pthread_rwlock_wrlock((latch)->val);    //写锁定读写锁
#define latch_unlock(latch) pthread_rwlock_unlock((latch)->val);   //解锁读写锁

#endif /* _latch_h_ */