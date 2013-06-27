/*
 * hash_index.h
 *
 *  Created on: 2011-11-29
 *      Author: Yan Shankai
 */

#ifndef HASH_INDEX_H_
#define HASH_INDEX_H_

#include <stdio.h>
#include "libyskdmu/util/log_util.h"

struct IndexItem {
	unsigned int record_id;
	IndexItem *next;
};

struct IndexHead {
	char *identifier;
	unsigned int key_info; //与identifier相关的信息
	unsigned int index_item_num; //索引项数量
	IndexItem *inverted_index; //倒排索引链表，存储此item在哪些record中出现过
};

typedef unsigned int (*HashFunc)(const char *key, unsigned int length, unsigned int table_size);
typedef unsigned int (*ProbeFunc)(const char *key, unsigned int length, unsigned int table_size, unsigned int collision_key, unsigned int probe_step);

/*
 * description: 哈希函数
 *  parameters: key:			关键字序列
 *  			  length:		关键字序列长度
 *  			  table_size:	哈希表长度
 *      return: 哈希值
 */
unsigned int hash(const char *key, unsigned int length, unsigned int table_size);

/*
 * description: 探查序列生成函数
 *  parameters: key:				关键字序列
 *  			  length:			关键字序列长度
 *  			  table_size:		哈希表长度
 *  			  collision_key:	碰撞的哈希值
 *  			  probe_step:		探查步数(0一般返回)
 *      return: 对应探查步数的探查值
 */
unsigned int probe(const char *key, unsigned int length, unsigned int table_size, unsigned int collision_key, unsigned int probe_step);

class HashIndex {
public:
	HashIndex();
	HashIndex(unsigned int table_size);
	HashIndex(unsigned int table_size, HashFunc hash_func, ProbeFunc probe_func);
	HashIndex(const HashIndex& hash_index);
	virtual ~HashIndex();

	/*
	 * description: 插入函数
	 *  parameters: key:			关键字
	 *  			  key_length: 	关键字长度
	 *  			  key_info:		详细项
	 *      return: 插入的槽对应的哈希值
	 */
	virtual unsigned int insert(const char *key, size_t key_length, unsigned int& key_info, unsigned int record_id);
	/*
	 * description: 获取索引大小
	 *      return: 索引所占内存大小，单位(byte)
	 */
	virtual unsigned int size_of_index();
	/*
	 * description: 获取记录数量函数
	 *  parameters: (in)		key:			关键字
	 *  			  			key_length:		关键字长度
	 *      return: 标记在索引头指针的记录数量
	 */
	virtual unsigned int get_mark_record_num(const char *key, size_t key_length);
	/*
	 * description: 获取实际记录数量函数
	 *  parameters: (in)		key:			关键字
	 *  			  			key_length:		关键字长度
	 *      return: 记录的实际数量
	 */
	virtual unsigned int get_real_record_num(const char *key, size_t key_length);
	/*
	 * description: 查找记录函数
	 *  parameters: (out)	records:			存放记录的缓冲区，使用后要记得释放内存
	 *  			  (in)		key:			关键字
	 *  						key_length:		关键字长度
	 *      return: 记录的数量，如果缓冲区已满(即比实际记录数小)则返回缓冲区大小
	 */
	virtual unsigned int find_record(unsigned int *records, const char *key, size_t key_length);
	/*
	 * description: 获取关键字信息
	 *  parameters: (out)	key_info:		详细项
	 *  			  (in)		key:		关键字
	 *  						key_length:	关键字长度
	 *      return: 关键字对应的关键字信息
	 */
	virtual bool get_key_info(unsigned int& key_info, const char *key, size_t key_length);
	/*
	 * description: 求记录索引项的交集函数(在保证索引链表有序的前提下用穿孔法)
	 *  parameters: keys:		关键字数组
	 *  			  key_num:	关键字数量
	 *      return: records[0]:	交集的元素个数
	 *      		  records[1]~records[record_num]: 交集的元素
	 */
	virtual unsigned int* get_intersect_records(const char **keys, unsigned int key_num);

//private:
	/*
	 * description: 哈希值生成函数
	 *  parameters: str:			关键字
	 *  			  length:		关键字长度
	 *  			  key_info:		关键字信息
	 *      return: 存放该item元组计数值在哈希表的位置
	 */
	virtual unsigned int hashfunc(const char *str, size_t length);
	/*
	 * description: 冲突处理函数
	 *  parameters: str:				关键字
	 *  			  length:			关键字长度
	 *  			  key_info:			关键字信息
	 *  			  collision_key:	冲突关键字
	 *      return: 不冲突的替代位置
	 */
	virtual unsigned int collision_handler(const char *str, size_t length, unsigned int collision_key);
	IndexHead** get_hash_table() { return m_hash_table; }

private:
	IndexItem* min_record_id(IndexItem **ptr, unsigned int ptr_num);

protected:
	IndexHead **m_hash_table;
	unsigned int m_table_size;
	HashFunc m_hash_func; //哈希函数指针
	ProbeFunc m_probe_func; //探查序列函数指针
	LogInstance* m_log_fp; //日志文件指针
};

#endif /* HASH_INDEX_H_ */
