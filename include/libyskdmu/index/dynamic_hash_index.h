/*
 * dynamic_hash_index.h
 *
 *  Created on: 2013-6-30
 *      Author: Yan Shankai
 */

#ifndef DYNAMIC_HASH_INDEX_H_
#define DYNAMIC_HASH_INDEX_H_

#include "libyskdmu/util/hashfunc_util.h"
#include "libyskdmu/index/ro_dhi_data.h"
#include "libyskdmu/index/closed_hash_index.h"

struct Catalog {
	unsigned int l;
	Bucket* bucket;
};

class DynamicHashIndex: public HashIndex {
public:
	friend class RODynamicHashIndexData;
	DynamicHashIndex(unsigned int bucket_size = 10,
			unsigned int global_deep = 2);
	DynamicHashIndex(const DynamicHashIndex& dynamic_hash_index);
	virtual ~DynamicHashIndex();

	/*
	 * description: 寻址函数
	 *  parameters: hashcode:	哈希值
	 *      return: 对应的目录项索引值
	 */
	virtual unsigned int addressing(unsigned int hashcode);
	/*
	 * description: 定位函数
	 *  parameters: key:		关键字
	 *  			key_length:	关键字长度
	 *      return: （对应的目录项索引值，目标在桶中的索引号(不存在返回-1)）
	 */
	virtual pair<unsigned int, int> locate_index(const char *key,
			size_t key_length);
	virtual unsigned int insert(const char *key, size_t key_length,
			char *key_info, unsigned int record_id);

	virtual bool split_bucket(unsigned int catalog_id, unsigned int local_deep =
			0);
	virtual bool split_catalog(unsigned int global_deep = 0);
	virtual unsigned int size_of_index();
	virtual unsigned int get_mark_record_num(const char *key,
			size_t key_length);
	virtual unsigned int get_real_record_num(const char *key,
			size_t key_length);
	virtual unsigned int find_record(unsigned int *records, const char *key,
			size_t key_length);
	virtual bool get_key_info(char **key_info, const char *key,
			size_t key_length);
	virtual unsigned int* get_intersect_records(const char **keys,
			unsigned int key_num);
	virtual bool change_key_info(const char *key, size_t key_length,
			const char* key_info);
#ifdef __DEBUG__
	virtual unsigned int hashfunc(const char *str, size_t length);
	virtual void* get_hash_table() {
		return m_catalogs;
	}
	unsigned int get_global_deep() {
		return m_d;
	}
	virtual void print_index(const char** identifiers = NULL);
#else

protected:
	virtual unsigned int hashfunc(const char *str, size_t length);
#endif

protected:
	unsigned int m_bucket_size; //桶大小
	unsigned int m_d; //全局位深度D
	Catalog* m_catalogs; //目录

private:
	unsigned int m_retry_times; //插入记录的重试次数
};

#endif /* DYNAMIC_HASH_INDEX_H_ */
