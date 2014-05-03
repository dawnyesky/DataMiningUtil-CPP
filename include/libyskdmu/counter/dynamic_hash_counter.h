/*
 * dynamic_hash_counter.h
 *
 *  Created on: 2014-5-2
 *      Author: Yan Shankai
 */

#ifndef DYNAMIC_HASH_COUNTER_H_
#define DYNAMIC_HASH_COUNTER_H_

#include <stdio.h>
#include "libyskdmu/util/hashfunc_util.h"
#include "libyskdmu/util/log_util.h"
#include "libyskdmu/counter/counter_interface.h"

namespace counter {

struct Bucket {
	vector<pair<unsigned int*, unsigned int> > elements;
};

struct Catalog {
	unsigned int l;
	Bucket* bucket;
};

}

class DynamicHashCounter: public Counter {
public:
	DynamicHashCounter(unsigned int size, unsigned int dimension,
			IHashFunc hash_func = (IHashFunc) &simple_hash);
	DynamicHashCounter(const DynamicHashCounter& counter);
	virtual ~DynamicHashCounter();
	virtual unsigned int addressing(unsigned int hashcode) const;
	virtual pair<unsigned int, int> locate_index(
			const unsigned int k_item[]) const;
	virtual bool split_bucket(unsigned int catalog_id, unsigned int local_deep =
			0);
	virtual bool split_catalog(unsigned int global_deep = 0);
	virtual bool count(const unsigned int k_item[], unsigned int num = 1);
	virtual unsigned int get_count(const unsigned int k_item[]) const;

protected:
	virtual unsigned int hashfunc(const unsigned int k_item[]) const;

protected:
	unsigned int m_dimension;
	unsigned int m_table_size;
	unsigned int m_bucket_size;
	unsigned int m_d;
	unsigned int m_retry_times;
	IHashFunc m_hash_func;
	counter::Catalog* m_catalogs;
};

#endif /* DYNAMIC_HASH_COUNTER_H_ */
