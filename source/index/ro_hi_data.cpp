/*
 * ro_hash_index.cpp
 *
 *  Created on: 2013-10-4
 *      Author: Yan Shankai
 */

#include "libyskdmu/index/ro_hi_data.h"

bool ROHashIndexData::is_built() {
	return m_is_built;
}

pair<void*, unsigned int> ROHashIndexData::get_index_data() {
	pair<void*, unsigned int> result;
	result.first = this->m_data;
	result.second = this->m_data_size;
	return result;
}
