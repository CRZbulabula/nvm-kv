1.  B+树里存一个logger对象。在B+树初始化时调用`RetCode init(const char *data_path, const char *log_path);`初始化，会返回显示是否打开文件成功。log_file的取名随意，给data_file后面加个啥或者换个名字也行，就是要注意上层传给B+树的数据文件名是带全路径的。在这个过程中logger会自己检查是否存在日志文件以及是否需要恢复。这部分代码在`logger.cpp`中你可以查看。
2. data_file和log_file这俩文件指针都是public暴露给b+树的，如果要**绕开logger**插入KV对中的value结点，可以直接用data_file，但是**要顺带把data_file_size改了**。
3. read时看一下`read_node`接口，和原来的`disk_read`几乎一样的，只是多了个NodeType和node的id

write稍微比较复杂

整个write的流程如下：
1. 进入写函数(在当前版本是`bplus_tree::insert_or_update`，不知道魔改优化后是啥)时调用`TransactionId open_transaction()`，保存拿到的`TransactionId`。这其实就是个`int`。
2. 把整个流程中的所有`disk_write`操作改成`write_node`，和read类似。只是你可能需要把上一步拿到的`TransactionId`传参给`search_index`和`search_leaf`之类的函数。
3. 在退出写函数前，调用一次`int commit_transaction(TransactionId tid)`。