/*
 * closed_hash_index.h
 *
 *  Created on: 2013-7-1
 *      Author: Yan Shankai
 */

#ifndef CLOSED_HASH_INDEX_H_
#define CLOSED_HASH_INDEX_H_

#include <vector>
#include "libyskdmu/index/hash_index.h"

struct Bucket {
	vector<IndexHead> elements;
};

#endif /* CLOSED_HASH_INDEX_H_ */
