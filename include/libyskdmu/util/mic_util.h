/*
 * mic_util.h
 *
 *  Created on: 2013-10-11
 *      Author: Yan Shankai
 */

#ifndef MIC_UTIL_H_
#define MIC_UTIL_H_

#include <vector>
#include "libyskdmu/macro.h"
#include "libyskdmu/index/ro_hash_index.h"

using namespace std;

/*
 * description: 合并两个项集
 *  parameters: (in)			itemset_1:		项集1
 *  			  				itemset_2:		项集2
 *      return: (out)	合并结果项集
 */
extern FUNCDECL vector<unsigned int>* union_set_mic(
		const vector<unsigned int>& itemset_1,
		const vector<unsigned int>& itemset_2);
/*
 * description: 求两个项集的自连接集
 *  parameters: (in)			itemset:		需要自连接的集合
 *      return: (out)	自连接集
 */
extern FUNCDECL vector<unsigned int>* union_eq_set_mic(
		const vector<unsigned int>& itemset_1,
		const vector<unsigned int>& itemset_2);
/*
 * description: 求两个项集的差集
 *  parameters: (in)			minuend:		被减集合
 *  			  				subtrahend:		减集合
 *      return: (out)	差集
 */
extern vector<unsigned int>* subtract_set(const vector<unsigned int>& minuend,
		const vector<unsigned int>& subtrahend);

extern FUNCDECL bool phi_filter_mic(vector<unsigned int>* k_itemset,
		unsigned int* support, ROHashIndex* ro_index, char* identifiers,
		unsigned int identifiers_size, unsigned int* id_index,
		unsigned int id_index_size, unsigned int minsup_count);

#endif /* MIC_UTIL_H_ */
