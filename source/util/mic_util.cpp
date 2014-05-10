/*
 * set_util.cpp
 *
 *  Created on: 2013-10-11
 *      Author: Yan Shankai
 */

#ifdef OMP
#pragma offload_attribute (push, target(mic))
#endif

#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/util/mic_util.h"

#ifdef OMP
#pragma offload_attribute (pop)
#endif

template FUNCDECL pair<unsigned int, bool> b_search<unsigned int>(
		const vector<unsigned int>& array, const unsigned int& element);

unsigned int* union_set_mic(unsigned int* itemset_1, unsigned int len_1,
		unsigned int* itemset_2, unsigned int len_2) {
	vector<unsigned int>* larger;
	vector<unsigned int>* smaller;
	if (len_1 > len_2) {
		larger = new vector<unsigned int>(itemset_1, itemset_1 + len_1);
		smaller = new vector<unsigned int>(itemset_2, itemset_2 + len_2);
	} else {
		larger = new vector<unsigned int>(itemset_2, itemset_2 + len_2);
		smaller = new vector<unsigned int>(itemset_1, itemset_1 + len_1);
	}
	vector<unsigned int>* result = larger;
	sort(result->begin(), result->end());
	for (unsigned int i = 0; i < smaller->size(); i++) {
		pair<unsigned int, bool> bs_result = b_search<unsigned int>(*larger,
				smaller->at(i));
		if (!bs_result.second) {
			result->insert(result->begin() + bs_result.first, smaller->at(i));
		}
	}
	unsigned int* result_array = new unsigned int[result->size() + 1];
	result_array[0] = result->size();
	memcpy(result_array + 1, result->data(),
			result->size() * sizeof(unsigned int));
	delete larger;
	delete smaller;
	return result_array;
}

unsigned int* union_eq_set_mic(unsigned int* itemset_1, unsigned int len_1,
		unsigned int* itemset_2, unsigned int len_2) {
	if (len_1 != len_2) {
		return NULL;
	}
	vector<unsigned int>* result = new vector<unsigned int>(itemset_1,
			itemset_1 + len_1);
	vector<unsigned int>* other = new vector<unsigned int>(itemset_2,
			itemset_2 + len_2);
	sort(result->begin(), result->end());
	sort(other->begin(), other->end());
	unsigned int i = 0;
	for (i = 0; i < result->size() - 1; i++) {
		if (result->at(i) != other->at(i)) {
			delete result;
			return NULL;
		}
	}
	if (result->at(i) <= other->at(i)) {
		result->push_back(other->at(i));
	} else {
		result->insert(result->begin() + i, other->at(i));
	}
	unsigned int* result_array = new unsigned int[result->size() + 1];
	result_array[0] = result->size();
	memcpy(result_array + 1, result->data(),
			result->size() * sizeof(unsigned int));
	delete result;
	delete other;
	return result_array;
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

bool phi_filter_mic(unsigned int* k_itemset, unsigned int* support,
		ROHashIndex* ro_index, char* identifiers, unsigned int identifiers_size,
		unsigned int* id_index, unsigned int id_index_size,
		unsigned int minsup_count) {
	char **keys = new char*[k_itemset[0]];
	if (0 == minsup_count)
		minsup_count = 1;
	unsigned int *result;
	for (unsigned int j = 0; j < k_itemset[0]; j++) {
		keys[j] = identifiers + id_index[k_itemset[j + 1]];
	}
	result = ro_index->get_intersect_records((const char **) keys,
			k_itemset[0]);
	*support = result[0];
	delete[] keys;
	delete[] result;

	return *support >= minsup_count;
}
