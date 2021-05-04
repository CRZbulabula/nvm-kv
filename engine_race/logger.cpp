#include "logger.h"

RetCode Logger::init(const char *data_path, const char *log_path, 
            size_t _meta_size, size_t _inter_size, size_t _leaf_size)
    {
        meta_size = _meta_size;
        inter_size = _inter_size;
        leaf_size = _leaf_size;
    }