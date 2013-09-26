/*
 * itemset.h
 *
 *  Created on: 2011-12-16
 *      Author: Yan Shankai
 */

#ifndef ITEMSET_H_
#define ITEMSET_H_

#include <vector>
#include <set>
#include "libyskdmu/index/hash_index.h"

using namespace std;

class KItemsets {
public:
	KItemsets();
	KItemsets(unsigned int term_num,
			unsigned int reserved_itemset_num = 1000000);
	KItemsets(const KItemsets& k_itemsets);
	virtual ~KItemsets();

	/*
	 * description: 合并两个项集
	 *  parameters: (in)			itemset_1:		项集1
	 *  			  				itemset_2:		项集2
	 *      return: (out)	合并结果项集
	 */
	static vector<unsigned int>* union_set(
			const vector<unsigned int>& itemset_1,
			const vector<unsigned int>& itemset_2);
	/*
	 * description: 求两个项集的差集
	 *  parameters: (in)			minuend:		被减集合
	 *  			  				subtrahend:		减集合
	 *      return: (out)	差集
	 */
	static vector<unsigned int>* subtract_set(
			const vector<unsigned int>& minuend,
			const vector<unsigned int>& subtrahend);
	/*
	 * description: 求两个项集的自连接集
	 *  parameters: (in)			itemset:		需要自连接的集合
	 *      return: (out)	自连接集
	 */
	static vector<unsigned int>* union_eq_set(
			const vector<unsigned int>& itemset_1,
			const vector<unsigned int>& itemset_2);
	/*
	 * description: 判断是否已存在此项集
	 *  parameters: itemset:	参考项集
	 *      return: 是否存在
	 */
	bool has_itemset(vector<unsigned int>& itemset);
	bool push(vector<unsigned int>& itemset, unsigned int support);
	void print() const;
	const map<vector<unsigned int>, unsigned int>& get_itemsets() const;
	pair<vector<unsigned int>, unsigned int>* pop();
	unsigned int get_term_num() const;

private:
	unsigned int m_term_num;
	map<vector<unsigned int>, unsigned int> m_itemsets;
	HashIndex* m_itemsets_index;
};

#endif /* ITEMSET_H_ */
