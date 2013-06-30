/*
 * hashfunc_util.cpp
 *
 *  Created on: 2013-6-30
 *      Author: Yan Shankai
 */

#include "libyskdmu/util/hashfunc_util.h"

unsigned int simple_hash(const char *key, unsigned int length,
		unsigned int table_size) {
	unsigned int hashcode = 0;
	for (unsigned int i = 0; i < length; i++) {
		hashcode = 37 * hashcode + key[i];
	}
	return hashcode %= table_size;
}
