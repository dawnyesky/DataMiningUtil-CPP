/*
 * search_util.h
 *
 *  Created on: 2011-12-10
 *      Author: Yan Shankai
 */

#ifndef SEARCH_UTIL_H_
#define SEARCH_UTIL_H_

#include <utility>
#include <vector>

using namespace std;

//找到目标后面一个或者比目标大的最小值(array必须是升序的，最好是构建array的一开始就是由b_search来寻找插入位置)
template<typename ItemType>
pair<unsigned int, bool> b_search(const vector<ItemType>& array,
		const ItemType& element) {
	if (array.size() == 0)
		return pair<unsigned int, bool>(0, false);
	unsigned int a = 0;
	unsigned int b = array.size() - 1;
	unsigned int c = 0;
	if (element < array[a]) {
		return pair<unsigned int, bool>(0, false);
	}
	if (element > array[b]) {
		return pair<unsigned int, bool>(b + 1, false);
	}
	while (1) {
		c = (a + b) / 2;
		if (c == a) {
			if (element == array[a]) {
				return pair<unsigned int, bool>(a, true);
			} else if (element == array[b]) {
				return pair<unsigned int, bool>(b, true);
			} else {
				return pair<unsigned int, bool>(b, false);
			}
		}
		if (element == array[c]) {
			return pair<unsigned int, bool>(c, true);
		} else if (element < array[c]) {
			b = c;
		} else {
			a = c;
		}
	}
}
#endif /* SEARCH_UTIL_H_ */
