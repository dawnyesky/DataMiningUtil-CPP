/*
 * hashfunc_util.h
 *
 *  Created on: 2013-6-30
 *      Author: Yan Shankai
 */

#ifndef HASHFUNC_UTIL_H_
#define HASHFUNC_UTIL_H_

#include "libyskdmu/macro.h"

typedef unsigned int (*HashFunc)(const char *key, unsigned int length,
		unsigned int table_size);

/*
 * description: 哈希函数
 *  parameters: key:			关键字序列
 *  			  length:		关键字序列长度
 *  			  table_size:	哈希表长度
 *      return: 哈希值
 */
unsigned int simple_hash(const char *key, unsigned int length,
		unsigned int table_size);

#endif /* HASHFUNC_UTIL_H_ */
