/*
 * hash_table_counter.h
 *
 *  Created on: 2011-12-1
 *      Author: Yan Shankai
 */

#ifndef HASH_TABLE_COUNTER_H_
#define HASH_TABLE_COUNTER_H_

#include <stdio.h>
#include "libyskdmu/util/hashfunc_util.h"
#include "libyskdmu/counter/counter_interface.h"

typedef unsigned int (*IProbeFunc)(const unsigned int *key, unsigned int length,
		unsigned int table_size, unsigned int collision_key,
		unsigned int probe_step);

/*
 * description: 哈希函数
 *  parameters: key:			关键字(整数)序列
 *  			  length:		关键字序列长度
 *  			  table_size:	哈希表长度
 *      return: 哈希值
 */
unsigned int hash(const unsigned int *key, unsigned int length,
		unsigned int table_size);

/*
 * description: 探查序列生成函数
 *  parameters: key:				关键字(整数)序列
 *  			  length:			关键字序列长度
 *  			  table_size:		哈希表长度
 *  			  collision_key:	碰撞的哈希值
 *  			  probe_step:		探查步数
 *      return: 对应探查步数的探查值
 */
unsigned int probe(const unsigned int *key, unsigned int length,
		unsigned int table_size, unsigned int collision_key,
		unsigned int probe_step);

/*
 * 计数表格
 * 用一个散列表保存元组(i, j,…… k, c)，其中i<j<……<k
 * 并且{i, j,…… k}是一个确实出现在一个或多个Record的项集
 * c是它的计数值，对(i, j,…… k)进行散列操作来寻找保存这个项集的桶的位置
 *
 * 当项目对出现的可能性小于1/3时适用
 */
class HashTableCounter: public Counter {
public:
	HashTableCounter(unsigned int size, unsigned int dimension,
			IHashFunc hash_func = (IHashFunc) &simple_hash,
			IProbeFunc probe_func = probe);
	HashTableCounter(const HashTableCounter& counter);
	virtual ~HashTableCounter();
	virtual bool count(const unsigned int k_item[], unsigned int num = 1);
	virtual unsigned int get_count(const unsigned int k_item[]) const;
protected:
	/*
	 * description: 哈希值生成函数
	 *  parameters: item元组的索引列表
	 *      return: 存放该item元组计数值在哈希表的位置
	 */
	virtual unsigned int hashfunc(const unsigned int k_item[]) const;
	/*
	 * description: 冲突处理函数
	 *  parameters: k_item:			item元组的索引列表
	 *  			  collision_key:	冲突关键字
	 *      return: 不冲突的替代位置
	 */
	virtual unsigned int collision_handler(const unsigned int k_item[],
			unsigned int collision_key) const;

private:
	unsigned int m_dimension;
	unsigned int **m_hash_table;
	unsigned int m_table_size;
	IHashFunc m_hash_func; //哈希函数指针
	IProbeFunc m_probe_func; //探查序列函数指针

	const static unsigned int MAX_TABLE_SIZE = 134217728; //哈希表最大的大小，索引数组占用了512M内存
};

#endif /* HASH_TABLE_COUNTER_H_ */
