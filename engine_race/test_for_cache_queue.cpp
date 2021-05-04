#include "CacheQueue.h"
#include "logger.h"

#include <iostream>

struct test {
    char s[100];
    int t[3];
};
int main()
{
    CacheQueue* cq = new CacheQueue(sizeof(test));
    test t;
    t.t[0] = 1;
    int s1 = cq->add_node(&t, true);
    t.t[0] = 2;
    int o1 = cq->add_node(&t, false);
    t.t[0] = 3;
    int o2 = cq->add_node(&t, false);
    cq->print_all();
    test t2;
    t2.t[0] = -1;
    cq->get_node(&t2, o1);
    printf("t2: %d\n", t2.t[0]);
    cq->sync_node(o1);
    cq->print_all();
}