/*
 * distributed_hash_index.h
 *
 *  Created on: 2013-7-10
 *      Author: Yan Shankai
 */

#ifndef DISTRIBUTED_HASH_INDEX_H_
#define DISTRIBUTED_HASH_INDEX_H_

#include "libyskdmu/index/dynamic_hash_index.h"

class DistributedHashIndex: public DynamicHashIndex {
public:
	/*
	 * description: 同步函数，把分布的哈希表合并并分割给每个节点负责一部分
	 *  parameters:
	 *      return: 同步是否完成
	 */
	virtual bool synchronize() = 0;
	/*
	 * description: 获取本节点负责的目录列表
	 *  parameters:
	 *      return: 本地目录列表
	 */
	virtual Catalog* get_local_catalogs() = 0;
	/*
	 * description: 获取本节点负责的所有索引项
	 *  parameters:
	 *      return: 本节点所有索引项
	 */
	virtual IndexHead* get_local_index() = 0;
	/*
	 * description: 获取全局索引大小
	 *  parameters:
	 *      return: 全局索引大小
	 */
	virtual unsigned int size_of_global_index() = 0;
};

#endif /* DISTRIBUTED_HASH_INDEX_H_ */
