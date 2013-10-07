/*
 * ro_dynamic_hash_index.h
 *
 *  Created on: 2013-10-4
 *      Author: Yan Shankai
 */

#ifndef RO_DYNAMIC_HASH_INDEX_H_
#define RO_DYNAMIC_HASH_INDEX_H_

#include "libyskdmu/macro.h"
#include "libyskdmu/index/ro_hash_index.h"

class CLASSDECL RODynamicHashIndex: public ROHashIndex {
public:
	RODynamicHashIndex();
	RODynamicHashIndex(unsigned int* deep, unsigned int* data,
			unsigned int* data_size, unsigned int* l1_index,
			unsigned int* l1_index_size, void* l2_index,
			unsigned int* l2_index_size, HashFunc hash_func = simple_hash_mic);
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
	/*
	 * description: 对象成员数据填充函数，用以一次获取类内所有成员数据
	 *  parameters: deep:			全局深度
	 *  			data:			索引数据区指针
	 *  			data_size:		索引数据区大小
	 *				l1_index:		一级索引指针
	 *				l1_index_size:	一级索引大小
	 *				l2_index：		二级索引指针
	 *				l2_index_size：	二级索引大小
	 *      return: 填充是否成功
	 */
	virtual bool fill_memeber_data(unsigned int** deep, unsigned int** data,
			unsigned int** data_size, unsigned int** l1_index,
			unsigned int** l1_index_size, void** l2_index,
			unsigned int** l2_index_size);
	virtual unsigned int get_record_num(const char *key, size_t key_length);
	virtual unsigned int find_record(unsigned int *records, const char *key,
			size_t key_length);
	virtual unsigned int* get_intersect_records(const char **keys,
			unsigned int key_num);
#ifdef __MIC__
	virtual bool offload_data();
	virtual bool recycle_data();
#endif

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

#endif /* RO_DYNAMIC_HASH_INDEX_H_ */
