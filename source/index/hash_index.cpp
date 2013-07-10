/*
 * hash_index.cpp
 *
 *  Created on: 2013-7-4
 *      Author: Yan Shankai
 */

#include "libyskdmu/index/hash_index.h"

IndexItem* HashIndex::min_record_id(IndexItem **ptr, unsigned int ptr_num) {
	if (ptr != NULL) {
		IndexItem* cur_min = ptr[0];
		for (unsigned int i = 1; i < ptr_num; i++) {
			if (ptr[i]->record_id < cur_min->record_id) {
				cur_min = ptr[i];
			}
		}
		return cur_min;
	} else {
		return NULL;
	}
}
