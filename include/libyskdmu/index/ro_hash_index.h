/*
 * ro_hash_index.h
 *
 *  Created on: 2013-10-4
 *      Author: Yan Shankai
 */

#ifndef RO_HASH_INDEX_H_
#define RO_HASH_INDEX_H_

#include <stdio.h>
#include "libyskdmu/util/hashfunc_util.h"
#include "libyskdmu/util/log_util.h"

FUNCDECL unsigned int simple_hash_mic(const char *key, unsigned int length,
		unsigned int table_size);

class CLASSDECL ROHashIndex {
public:
	virtual ~ROHashIndex() {
	}

	/*
	 * description: 获取记录数量函数
	 *  parameters: (in)	key:			关键字
	 *  			  		key_length:		关键字长度
	 *      return: 记录数量
	 */
	virtual unsigned int get_record_num(const char *key, size_t key_length) = 0;
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
	 * description: 求记录索引项的交集函数
	 *  parameters: keys:		关键字数组
	 *  			key_num:	关键字数量
	 *      return: records[0]:						交集的元素个数
	 *      		records[1]~records[record_num]: 交集的元素
	 */
	virtual unsigned int* get_intersect_records(const char **keys,
			unsigned int key_num) = 0;
	/*
	 * description: 把数据加载到加速卡
	 *      return: 加载数据是否成功
	 */
	virtual bool offload_data() = 0;
	/*
	 * description: 把加载到加速卡的数据所占内存空间回收
	 *      return: 回收内存是否成功
	 */
	virtual bool recycle_data() = 0;

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
	void* m_data; //索引数据块
	unsigned int m_data_size; //索引数据块大小
	HashFunc m_hash_func; //哈希函数指针
	bool m_is_built; //是否已经构建
};

#endif /* RO_HASH_INDEX_H_ */
