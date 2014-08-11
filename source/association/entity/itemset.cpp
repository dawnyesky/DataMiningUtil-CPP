/*
 * itemset.cpp
 *
 *  Created on: 2011-12-16
 *      Author: Yan Shankai
 */

#include <algorithm>
#include <limits>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/index/dynamic_hash_index.h"
#include "libyskdmu/association/entity/itemset.h"

KItemsets::KItemsets() {
	m_term_num = 1;
	m_itemsets_index = new DynamicHashIndex();
}

KItemsets::KItemsets(unsigned int term_num, unsigned int reserved_itemset_num) :
		m_term_num(term_num) {
	const unsigned int bucket_size = 256;
	m_itemsets_index = new DynamicHashIndex(bucket_size,
			std::max((int) (log(reserved_itemset_num / bucket_size) / log(2)),
					2));
}

KItemsets::KItemsets(const KItemsets& k_itemsets) :
		m_itemsets(k_itemsets.m_itemsets) {
	m_term_num = k_itemsets.m_term_num;
	m_itemsets_index = new DynamicHashIndex(
			*(DynamicHashIndex*) k_itemsets.m_itemsets_index);
}

KItemsets::~KItemsets() {
	if (m_itemsets_index != NULL) {
		delete m_itemsets_index;
	}
}

vector<unsigned int>* KItemsets::union_set(
		const vector<unsigned int>& itemset_1,
		const vector<unsigned int>& itemset_2) {
	const vector<unsigned int>* larger;
	const vector<unsigned int>* smaller;
	if (itemset_1.size() > itemset_2.size()) {
		larger = &itemset_1;
		smaller = &itemset_2;
	} else {
		larger = &itemset_2;
		smaller = &itemset_1;
	}
	vector<unsigned int>* result = new vector<unsigned int>(*larger);
	sort(result->begin(), result->end());
	for (unsigned int i = 0; i < smaller->size(); i++) {
		pair<unsigned int, bool> bs_result = b_search<unsigned int>(*larger,
				smaller->at(i));
		if (!bs_result.second) {
			result->insert(result->begin() + bs_result.first, smaller->at(i));
		}
	}
	return result;
}

vector<unsigned int>* KItemsets::subtract_set(
		const vector<unsigned int>& minuend,
		const vector<unsigned int>& subtrahend) {
	const vector<unsigned int>* larger;
	const vector<unsigned int>* smaller;
	if (minuend.size() > subtrahend.size()) {
		larger = &minuend;
		smaller = &subtrahend;
	} else {
		larger = &subtrahend;
		smaller = &minuend;
	}
	vector<unsigned int>* result = new vector<unsigned int>(*larger);
	sort(result->begin(), result->end());
	for (unsigned int i = 0; i < smaller->size(); i++) {
		pair<unsigned int, bool> bs_result = b_search<unsigned int>(*larger,
				smaller->at(i));
		if (bs_result.second) {
			if (bs_result.first > result->size() - 1)
				continue;
			result->erase(result->begin() + bs_result.first);
		}
	}
	return result;
}

vector<unsigned int>* KItemsets::union_eq_set(
		const vector<unsigned int>& itemset_1,
		const vector<unsigned int>& itemset_2) {
	if (itemset_1.size() != itemset_2.size()) {
		return NULL;
	}
	vector<unsigned int>* result = new vector<unsigned int>(itemset_1);
	vector<unsigned int> other = vector<unsigned int>(itemset_2);
	sort(result->begin(), result->end());
	sort(other.begin(), other.end());
	unsigned int i = 0;
	for (i = 0; i < result->size() - 1; i++) {
		if (result->at(i) != other[i]) {
			delete result;
			return NULL;
		}
	}
	if (result->at(i) <= other[i]) {
		result->push_back(other[i]);
	} else {
		result->insert(result->begin() + i, other[i]);
	}
	return result;
}

bool KItemsets::has_itemset(vector<unsigned int>& itemset) {
	if (itemset.size() == m_term_num
			|| m_term_num == numeric_limits<unsigned int>::max()) {
		sort(itemset.begin(), itemset.end());
		char* key_info = NULL;
		char *key = ivtoa((int*) itemset.data(), itemset.size(), ",", 10);
		if (m_itemsets_index->get_key_info(&key_info, key, strlen(key))) {
			if (key != NULL)
				delete[] key;
			return true;
		}
		if (key != NULL)
			delete[] key;
		if (key_info != NULL) {
			delete[] key_info;
		}
		return false;
	} else {
		return false;
	}
}

bool KItemsets::push(vector<unsigned int>& itemset, unsigned int support) {
	if (itemset.size() == m_term_num
			|| m_term_num == numeric_limits<unsigned int>::max()) {
		//先令k-term里的元素有序
		sort(itemset.begin(), itemset.end());
		m_itemsets.insert(
				pair<vector<unsigned int>, unsigned int>(itemset, support));
		//添加索引用以去重
		const char *key = ivtoa((int*) itemset.data(), itemset.size(), ",", 10);
		char key_info = ' ';
		m_itemsets_index->insert(key, strlen(key), &key_info, 0);
		if (key != NULL)
			delete[] key;
		return true;
	} else {
		printf("itemset's size is: %u ; KItemsets term num is: %u\n",
				itemset.size(), m_term_num);
		return false;
	}
}

void KItemsets::print() const {
	for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
			m_itemsets.begin(); iter != m_itemsets.end(); iter++) {
		printf("{ %d", iter->first[0]);
		for (unsigned int i = 1; i < iter->first.size(); i++) {
			printf(", %d", iter->first[i]);
		}
		printf(" }\n");
	}
}

const map<vector<unsigned int>, unsigned int>& KItemsets::get_itemsets() const {
	return m_itemsets;
}

pair<vector<unsigned int>, unsigned int>* KItemsets::pop() {
	pair<vector<unsigned int>, unsigned int>* itemset = new pair<
			vector<unsigned int>, unsigned int>(m_itemsets.begin()->first,
			m_itemsets.begin()->second);
	m_itemsets.erase(m_itemsets.begin());
	return itemset;
}

unsigned int KItemsets::get_term_num() const {
	return m_term_num;
}

void KItemsets::set_term_num(unsigned int term_num) {
	m_term_num = term_num;
}
