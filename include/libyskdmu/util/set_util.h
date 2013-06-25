/*
 * set_util.h
 *
 *  Created on: 2012-5-25
 *      Author: "Yan Shankai"
 */

#ifndef SET_UTIL_H_
#define SET_UTIL_H_

#include <vector>
#include "libyskdmu/util/search_util.h"

using namespace std;

template<typename ItemType>
bool is_subset(const vector<ItemType>& super, const vector<ItemType>& sub) {
	if (super.size() == 0 || sub.size() == 0)
		return false;
	if (sub.front() < super.front() || sub.back() > super.back())
		return false;
	for (unsigned int i = 0, next = 0; i < sub.size(); i++) {
		vector<ItemType> array = vector<ItemType>(super.begin() + next, super.end());
		pair<unsigned int, bool> found_item = b_search<ItemType>(array, sub[i]);
		if (found_item.second) {
			next = found_item.first + 1;
		} else {
			return false;
		}
	}
	return true;
}

#endif /* SET_UTIL_H_ */
