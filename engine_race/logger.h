#ifndef LOGGER_H_
#define LOGGER_H_

#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <unordered_map>

//#include "include/engine.h"
#include "../include/engine.h"

using polar_race::RetCode;

enum NodeType{
    meta,
    inter,
    leaf,
};

struct logBlock {
    char* log;
    off_t offset;
    size_t size;  
};

//省点空间，不要 off_t 和 size_t 了
struct logItem {
    int offset; //数据文件中的偏移
    int length; //数据文件中的长度
    int timestamp; //写入的时间戳
    int node_id; //结点id
    int cache_id; //对应的cache的id
    NodeType type; //结点类型
};

//log文件最大 4MB
#define LOG_SIZE_LIMIT (1<<22)

//log函数的前四个字节标记log是否已提交
//如不正确说明log未提交，需要恢复
const int COMMITTED_CODE = 4396;
const int NOT_SUBMIT_CODE = 443;

typedef int TransactionId;
typedef std::vector<logItem> Transaction;

#endif