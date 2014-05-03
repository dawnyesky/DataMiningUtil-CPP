/*
 * dynamic_hash_counter.cpp
 *
 *  Created on: 2014-5-2
 *      Author: Yan Shankai
 */

#include <string.h>
#include <stdlib.h>
#include <cmath>
#include "libyskalgrthms/math/arrange_combine.h"
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/counter/dynamic_hash_counter.h"

DynamicHashCounter::DynamicHashCounter(unsigned int size,
		unsigned int dimension, IHashFunc hash_func) {
	m_dimension = dimension;
	m_size = size;
	m_table_size = combine(m_size, m_dimension);
	m_bucket_size = 256;
	m_d = std::max((int) (std::log(m_table_size / m_bucket_size) / std::log(2)),
			2);
	m_catalogs = new counter::Catalog[(int) std::pow(2, m_d)];
	for (unsigned int i = 0; i < (unsigned int) pow(2, m_d); i++) {
		m_catalogs[i].l = m_d;
		m_catalogs[i].bucket = new counter::Bucket();
	}
	m_retry_times = 100;
	m_hash_func = hash_func;
	m_log_fp = LogUtil::get_instance()->get_log_instance("dynamicHashCounter");
}

DynamicHashCounter::DynamicHashCounter(const DynamicHashCounter& counter) {
	m_hash_func = counter.m_hash_func;
	m_dimension = counter.m_dimension;
	m_size = counter.m_size;
	m_table_size = counter.m_table_size;
	m_bucket_size = counter.m_bucket_size;
	m_d = counter.m_d;
	m_retry_times = counter.m_retry_times;
	m_log_fp = counter.m_log_fp;
	m_catalogs = new counter::Catalog[(int) pow(2, m_d)];
	for (unsigned int i = 0; i < (unsigned int) pow(2, m_d); i++) {
		m_catalogs[i].l = counter.m_catalogs[i].l;
		m_catalogs[i].bucket = new counter::Bucket();
		vector<pair<unsigned int*, unsigned int> >& elements =
				counter.m_catalogs[i].bucket->elements;
		for (unsigned int j = 0; j < elements.size(); j++) {
			unsigned int* k_item = new unsigned int[m_dimension];
			memcpy(k_item, elements[j].first,
					m_dimension * sizeof(unsigned int));
			m_catalogs[i].bucket->elements.push_back(
					pair<unsigned int*, unsigned int>(k_item,
							elements[j].second));
		}
	}
}

DynamicHashCounter::~DynamicHashCounter() {
	unsigned int catalogs_size = pow(2, m_d);
	unsigned char* deleted = new unsigned char[catalogs_size];
	memset(deleted, 0, catalogs_size * sizeof(unsigned char));
	for (unsigned int i = 0; i < catalogs_size; i++) {
		if (deleted[i] == 0) {
			for (vector<pair<unsigned int*, unsigned int> >::iterator iter =
					m_catalogs[i].bucket->elements.begin();
					iter != m_catalogs[i].bucket->elements.end(); iter++) {
				if (NULL != iter->first) {
					delete[] iter->first;
				}
			}
			delete m_catalogs[i].bucket;
			for (unsigned int j = 0;
					j < (unsigned int) pow(2, m_d - m_catalogs[i].l); j++) {
				deleted[i + j * (int) pow(2, m_catalogs[i].l)] = 1;
			}
		}
	}
	if (m_catalogs != NULL) {
		delete[] m_catalogs;
	}
	delete[] deleted;
}

unsigned int DynamicHashCounter::addressing(unsigned int hashcode) const {
	unsigned int mask = 0x01;
	for (unsigned int i = 1; i < m_d; i++) {
		mask |= 0x01 << i;
	}
	return hashcode & mask;
}

pair<unsigned int, int> DynamicHashCounter::locate_index(
		const unsigned int k_item[]) const {
	unsigned int hashcode = hashfunc(k_item);
	unsigned int catalog_id = addressing(hashcode);
	vector<pair<unsigned int*, unsigned int> >& elements =
			m_catalogs[catalog_id].bucket->elements;
	pair<unsigned int, int> result;
	result.first = catalog_id;
	result.second = -1;
	for (unsigned int i = 0; i < elements.size(); i++) {
		if (memcmp(elements[i].first, k_item,
				m_dimension * sizeof(unsigned int)) == 0) {
			result.second = i;
			break;
		}
	}
	return result;
}

bool DynamicHashCounter::split_bucket(unsigned int catalog_id,
		unsigned int local_deep) {
	if (catalog_id > pow(2, m_d) - 1) { //目录索引越界
		return false;
	}
	if (m_d == m_catalogs[catalog_id].l) { //局部深度与全局深度相等
		return false;
	}
	counter::Bucket* old_bucket = m_catalogs[catalog_id].bucket;
	if (local_deep == 0) { //默认分裂方式
		//调整目录的桶指向
		unsigned int old_deep = m_catalogs[catalog_id].l;
		for (unsigned int i = 0; i < 2; i++) {
			counter::Bucket* new_bucket = new counter::Bucket();
			for (unsigned int j = 0;
					j < (unsigned int) pow(2, m_d - old_deep - 1); j++) {
				unsigned int cid = (catalog_id + i * (int) pow(2, old_deep)
						+ j * (int) pow(2, old_deep + 1)) % (int) pow(2, m_d);
				m_catalogs[cid].bucket = new_bucket;
				m_catalogs[cid].l++;
			}
		}
	} else { //按指定局部深度分裂
		if (local_deep > m_d)
			return false;
		else if (local_deep < m_catalogs[catalog_id].l)
			return false;
		else if (local_deep == m_catalogs[catalog_id].l)
			return true;
		//调整目录的桶指向
		unsigned int old_deep = m_catalogs[catalog_id].l;
		for (unsigned int i = 0;
				i < (unsigned int) pow(2, local_deep - old_deep); i++) {
			counter::Bucket* new_bucket = new counter::Bucket();
			for (unsigned int j = 0;
					j < (unsigned int) pow(2, m_d - local_deep); j++) {
				unsigned int cid = (catalog_id + i * (int) pow(2, old_deep)
						+ j * (int) pow(2, local_deep)) % (int) pow(2, m_d);
				m_catalogs[cid].bucket = new_bucket;
				m_catalogs[cid].l = local_deep;
			}
		}
	}
	//重新分配旧桶的元素
	for (vector<pair<unsigned int*, unsigned int> >::iterator iter =
			old_bucket->elements.begin(); iter != old_bucket->elements.end();
			iter++) {
		m_catalogs[addressing(hashfunc(iter->first))].bucket->elements.push_back(
				*iter);
	}
	delete old_bucket;
	return true;
}

bool DynamicHashCounter::split_catalog(unsigned int global_deep) {
	unsigned int old_d = m_d;
	counter::Catalog* old_catalogs = m_catalogs;
	if (global_deep == 0) { //默认分裂方式
		m_d++;
		//目录分裂成2倍
		m_catalogs = new counter::Catalog[(int) pow(2, m_d)];
		//索引的最后(m_d-1)位相同的目录指向原目录的Bucket
		for (unsigned int i = 0; i < (unsigned int) pow(2, old_d); i++) {
			m_catalogs[i].bucket = old_catalogs[i].bucket;
			m_catalogs[i].l = old_catalogs[i].l;
			unsigned int cid = i + (int) pow(2, old_d);
			m_catalogs[cid].bucket = old_catalogs[i].bucket;
			m_catalogs[cid].l = old_catalogs[i].l;
		}
	} else { //按指定全局深度分裂
		if (global_deep < m_d)
			return false;
		else if (global_deep == m_d)
			return true;
		m_d = global_deep;
		//目录分裂成2^(global-m_d)倍
		m_catalogs = new counter::Catalog[(int) pow(2, m_d)];
		//索引的最后(m_d-1)位相同的目录指向原目录的Bucket
		for (unsigned int i = 0; i < pow(2, old_d); i++) {
			for (unsigned int j = 0; j < pow(2, m_d - old_d); j++) {
				unsigned int cid = i + j * (int) pow(2, old_d);
				m_catalogs[cid].bucket = old_catalogs[i].bucket;
				m_catalogs[cid].l = old_catalogs[i].l;
			}
		}
	}
	delete[] old_catalogs;
	return true;
}

bool DynamicHashCounter::count(const unsigned int k_item[], unsigned int num) {
	unsigned int temp_k_item[m_dimension];
	memcpy(temp_k_item, k_item, m_dimension * sizeof(unsigned int));
	if (quicksort<unsigned int>(temp_k_item, m_dimension, true, true)
			!= m_dimension)
		return false;
	pair<unsigned int, int> location = locate_index(temp_k_item);
	if (location.second == -1) {
		unsigned int hashcode = hashfunc(temp_k_item);
		unsigned int catalog_id = location.first;
		counter::Bucket* bucket = m_catalogs[catalog_id].bucket;
		unsigned int retry_times = 0;
		while (bucket->elements.size() >= m_bucket_size
				&& retry_times <= m_retry_times) { //如果桶一直是满的且还没超过重试次数则继续分裂
			if (!split_bucket(catalog_id)) {
				if (!split_catalog()) {
					continue;
				} else {
					catalog_id = addressing(hashcode);
					split_bucket(catalog_id);
				}
			}
			catalog_id = addressing(hashcode);
			bucket = m_catalogs[catalog_id].bucket;
			retry_times++;
		}

		if (bucket->elements.size() >= m_bucket_size) { //超出分裂桶的尝试个数，尝试调整哈希函数
			m_log_fp->error("Splitting bucket time out!");
			exit(-1);
		}

		catalog_id = addressing(hashcode);
		bucket = m_catalogs[catalog_id].bucket;
		unsigned int* k_item = new unsigned int[m_dimension];
		memcpy(k_item, temp_k_item, m_dimension * sizeof(unsigned int));
		bucket->elements.push_back(
				pair<unsigned int*, unsigned int>(k_item, num));
	} else {
		m_catalogs[location.first].bucket->elements[location.second].second +=
				num;
	}
	return true;
}

unsigned int DynamicHashCounter::get_count(const unsigned int k_item[]) const {
	unsigned int temp_k_item[m_dimension];
	memcpy(temp_k_item, k_item, m_dimension * sizeof(unsigned int));
	if (quicksort<unsigned int>(temp_k_item, m_dimension, true, true)
			!= m_dimension)
		return false;
	pair<unsigned int, int> location = locate_index(temp_k_item);
	if (location.second == -1) {
		return 0;
	} else {
		return m_catalogs[location.first].bucket->elements[location.second].second;
	}
}

unsigned int DynamicHashCounter::hashfunc(const unsigned int k_item[]) const {
	return m_hash_func(k_item, m_dimension, m_table_size);
}
