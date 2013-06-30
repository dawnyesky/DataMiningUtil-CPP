/*
 * hash_index_interface.h
 *
 *  Created on: 2011-11-29
 *      Author: Yan Shankai
 */

#ifndef HASH_INDEX_INTERFACE_H_
#define HASH_INDEX_INTERFACE_H_

#include <stdio.h>
#include "libyskdmu/util/hashfunc_util.h"
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

class HashIndex {
public:
	virtual ~HashIndex() {
	}
	;

	/*
	 * description: 插入函数
	 *  parameters: key:			关键字
	 *  			key_length: 	关键字长度
	 *  			key_info:		详细项
	 *      return: 插入的槽对应的哈希值
	 */
	virtual unsigned int insert(const char *key, size_t key_length,
			unsigned int& key_info, unsigned int record_id) = 0;
	/*
	 * description: 获取索引大小
	 *      return: 索引所占内存大小，单位(byte)
	 */
	virtual unsigned int size_of_index() = 0;
	/*
	 * description: 获取记录数量函数
	 *  parameters: (in)	key:			关键字
	 *  			  		key_length:		关键字长度
	 *      return: 标记在索引头指针的记录数量
	 */
	virtual unsigned int get_mark_record_num(const char *key,
			size_t key_length) = 0;
	/*
	 * description: 获取实际记录数量函数
	 *  parameters: (in)	key:			关键字
	 *  			  		key_length:		关键字长度
	 *      return: 记录的实际数量
	 */
	virtual unsigned int get_real_record_num(const char *key,
			size_t key_length) = 0;
	/*
	 * description: 查找记录函数
	 *  parameters: (out)	records:		存放记录的缓冲区，使用后要记得释放内存
	 *  			(in)	key:			关键字
	 *  					key_length:		关键字长度
	 *      return: 记录的数量，如果缓冲区已满(即比实际记录数小)则返回缓冲区大小
	 */
	virtual unsigned int find_record(unsigned int *records, const char *key,
			size_t key_length) = 0;
	/*
	 * description: 获取关键字信息
	 *  parameters: (out)	key_info:	详细项
	 *  			(in)	key:		关键字
	 *  					key_length:	关键字长度
	 *      return: 关键字对应的关键字信息
	 */
	virtual bool get_key_info(unsigned int& key_info, const char *key,
			size_t key_length) = 0;
	/*
	 * description: 求记录索引项的交集函数(在保证索引链表有序的前提下用穿孔法)
	 *  parameters: keys:		关键字数组
	 *  			key_num:	关键字数量
	 *      return: records[0]:						交集的元素个数
	 *      		records[1]~records[record_num]: 交集的元素
	 */
	virtual unsigned int* get_intersect_records(const char **keys,
			unsigned int key_num) = 0;

protected:
	/*
	 * description: 哈希值生成函数
	 *  parameters: str:		关键字
	 *  			length:		关键字长度
	 *  			key_info:	关键字信息
	 *      return: 存放该item元组计数值在哈希表的位置
	 */
	virtual unsigned int hashfunc(const char *str, size_t length) = 0;

protected:
	unsigned int m_table_size; //哈希表大小
	HashFunc m_hash_func; //哈希函数指针
	LogInstance* m_log_fp; //日志文件指针
};

#endif /* HASH_INDEX_INTERFACE_H_ */
