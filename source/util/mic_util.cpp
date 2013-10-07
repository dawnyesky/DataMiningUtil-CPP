/*
 * set_util.cpp
 *
 *  Created on: 2013-10-11
 *      Author: Yan Shankai
 */

#include <stdlib.h>
#include <algorithm>
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/util/mic_util.h"

vector<unsigned int>* union_set_mic(const vector<unsigned int>& itemset_1,
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

vector<unsigned int>* union_eq_set_mic(const vector<unsigned int>& itemset_1,
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

vector<unsigned int>* subtract_set(const vector<unsigned int>& minuend,
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

bool phi_filter_mic(vector<unsigned int>* k_itemset, unsigned int* support,
		ROHashIndex* ro_index, char* identifiers, unsigned int identifiers_size,
		unsigned int* id_index, unsigned int id_index_size,
		unsigned int minsup_count) {
	char **keys = new char*[k_itemset->size()];
	if (0 == minsup_count)
		minsup_count = 1;
	unsigned int *result;
	for (unsigned int j = 0; j < k_itemset->size(); j++) {
		keys[j] = identifiers + id_index[k_itemset->at(j)];
	}
	result = ro_index->get_intersect_records((const char **) keys,
			k_itemset->size());
	*support = result[0];
	delete[] keys;
	delete[] result;

	return *support >= minsup_count;
}
