/*
 * dynamic_hash_index.h
 *
 *  Created on: 2013-6-30
 *      Author: Yan Shankai
 */

#ifndef DYNAMIC_HASH_INDEX_H_
#define DYNAMIC_HASH_INDEX_H_

#include "libyskdmu/util/hashfunc_util.h"
#include "libyskdmu/index/hash_index_interface.h"

class DynamicHashIndex: HashIndex {
public:
	DynamicHashIndex(unsigned int bucket_size = 10,
			unsigned int global_deep = 2);
	DynamicHashIndex(const DynamicHashIndex& dynamic_hash_index);
	virtual ~DynamicHashIndex();

	bool init(HashFunc hash_func = simple_hash);
	virtual unsigned int insert(const char *key, size_t key_length,
			unsigned int& key_info, unsigned int record_id);
	virtual unsigned int size_of_index();
	virtual unsigned int get_mark_record_num(const char *key,
			size_t key_length);
	virtual unsigned int get_real_record_num(const char *key,
			size_t key_length);
	virtual unsigned int find_record(unsigned int *records, const char *key,
			size_t key_length);
	virtual bool get_key_info(unsigned int& key_info, const char *key,
			size_t key_length);
	virtual unsigned int* get_intersect_records(const char **keys,
			unsigned int key_num);
#ifdef __DEBUG__
	virtual unsigned int hashfunc(const char *str, size_t length);
	void* get_hash_table() {
		return NULL;
	}
#else

protected:
	/*
	 * description: 哈希值生成函数
	 *  parameters: str:			关键字
	 *  			  length:		关键字长度
	 *  			  key_info:		关键字信息
	 *      return: 存放该item元组计数值在哈希表的位置
	 */
	virtual unsigned int hashfunc(const char *str, size_t length);
#endif

protected:
	unsigned int m_bucket_size; //桶大小
	unsigned int m_d; //全局位深度D
};

#endif /* DYNAMIC_HASH_INDEX_H_ */
