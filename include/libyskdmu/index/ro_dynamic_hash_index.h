/*
 * ro_dynamic_hash_index.h
 *
 *  Created on: 2013-10-4
 *      Author: Yan Shankai
 */

#ifndef RO_DYNAMIC_HASH_INDEX_H_
#define RO_DYNAMIC_HASH_INDEX_H_

#include "libyskdmu/index/hash_index.h"
#include "libyskdmu/index/ro_hash_index.h"

#pragma offload_attribute(push, target(mic))
class RODynamicHashIndex: public ROHashIndex {
public:
	RODynamicHashIndex();
	virtual ~RODynamicHashIndex();

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
	 *      return: （对应的目录项索引值，返回在索引数据区的偏移(不存在则返回NULL)）
	 */
	virtual pair<unsigned int, void*> locate_index(const char *key,
			size_t key_length);
	virtual bool build(HashIndex* original_index);
	virtual unsigned int get_record_num(const char *key, size_t key_length);
	virtual unsigned int find_record(unsigned int *records, const char *key,
			size_t key_length);
	virtual unsigned int* get_intersect_records(const char **keys,
			unsigned int key_num);
	virtual bool offload_data();
	virtual bool recycle_data();

#ifndef __DEBUG__
protected:
#endif
	virtual unsigned int hashfunc(const char *str, size_t length);
	void print_l1_index();
	void print_l2_index();
	void print_index_data();

protected:
	unsigned int m_d; //全局位深度D
	unsigned int* m_l1_index; //索引数据块的一级索引
	unsigned int m_l1_index_size; //一级索引大小
	void* m_l2_index; //索引数据块的二级索引
	unsigned int m_l2_index_size;
};
#pragma offload_attribute(pop)

#endif /* RO_DYNAMIC_HASH_INDEX_H_ */
