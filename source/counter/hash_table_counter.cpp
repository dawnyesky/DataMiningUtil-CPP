/*
 * hash_table_counter.cpp
 *
 *  Created on: 2011-12-1
 *      Author: Yan Shankai
 */

#include <stdio.h>
#include <string.h>
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskalgrthms/math/arrange_combine.h"
#include "libyskdmu/counter/hash_table_counter.h"

unsigned int hash(const unsigned int *key, unsigned int length,
		unsigned int table_size) {
	unsigned int hashcode = 0;
	for (unsigned int i = 0; i < length; i++) {
		hashcode = 37 * hashcode + key[i];
	}
	return hashcode %= table_size;
}

unsigned int probe(const unsigned int *key, unsigned int length,
		unsigned int table_size, unsigned int collision_key,
		unsigned int probe_step) {
	return (collision_key + probe_step * probe_step) % table_size;
}

HashTableCounter::HashTableCounter(unsigned int size, unsigned int dimension) {
	m_dimension = dimension;
	m_size = size;
	m_table_size = combine(m_size, m_dimension) * 1.5; //哈希表大小
	if (0 == m_table_size)
		m_table_size = 100;
	if (m_table_size > MAX_TABLE_SIZE) {
		m_table_size = MAX_TABLE_SIZE;
	}
	m_hash_table = new unsigned int*[m_table_size]; //建立哈希表
	for (unsigned int i = 0; i < m_table_size; i++) {
		m_hash_table[i] = NULL;
	}
	m_hash_func = &hash;
	m_probe_func = &probe;
	this->m_log_fp = LogUtil::get_instance()->get_log_instance(
			"hashTableCounter");
}

HashTableCounter::HashTableCounter(unsigned int size, unsigned int dimension,
		IHashFunc hash_func, IProbeFunc probe_func) {
	m_dimension = dimension;
	m_size = size;
	m_table_size = combine(m_size, m_dimension) * 1.5; //哈希表大小
	if (m_table_size > MAX_TABLE_SIZE) {
		m_table_size = MAX_TABLE_SIZE;
	}
	m_hash_table = new unsigned int*[m_table_size]; //建立哈希表
	for (unsigned int i = 0; i < m_table_size; i++) {
		m_hash_table[i] = NULL;
	}
	m_hash_func = (NULL == hash_func ? &hash : hash_func);
	m_probe_func = (NULL == probe_func ? &probe : probe_func);
	this->m_log_fp = LogUtil::get_instance()->get_log_instance(
			"hashTableCounter");
}

HashTableCounter::HashTableCounter(const HashTableCounter& counter) {
	m_dimension = counter.m_dimension;
	m_size = counter.m_size;
	m_table_size = combine(m_size, m_dimension) * 1.5; //哈希表大小
	if (m_table_size > MAX_TABLE_SIZE) {
		m_table_size = MAX_TABLE_SIZE;
	}
	m_hash_table = new unsigned int*[m_table_size]; //建立哈希表
	for (unsigned int i = 0; i < m_table_size; i++) {
		m_hash_table[i] = NULL;
	}
	m_hash_func = counter.m_hash_func;
	m_probe_func = counter.m_probe_func;
	this->m_log_fp = LogUtil::get_instance()->get_log_instance(
			"hashTableCounter");
}

HashTableCounter::~HashTableCounter() {
	for (unsigned int i = 0; i < m_table_size; i++) {
		if (NULL != m_hash_table[i])
			delete[] m_hash_table[i];
	}
	delete[] m_hash_table;
}

bool HashTableCounter::count(const unsigned int k_item[]) {
	unsigned int temp_k_item[m_dimension];
	for (unsigned int i = 0; i < m_dimension; i++) {
		temp_k_item[i] = k_item[i];
	}
	if (quicksort<unsigned int>(temp_k_item, m_dimension, true, true)
			!= m_dimension)
		return false;
	unsigned int hash_code = hashfunc(temp_k_item);
	if (NULL != m_hash_table[hash_code]) {
		m_hash_table[hash_code][m_dimension]++;
	} else {
		m_hash_table[hash_code] = new unsigned int[m_dimension + 1];
		for (unsigned int i = 0; i < m_dimension; i++) {
			m_hash_table[hash_code][i] = temp_k_item[i];
		}
		m_hash_table[hash_code][m_dimension] = 1;
	}
	return true;
}

bool HashTableCounter::count(const unsigned int k_item[], unsigned int num) {
	unsigned int temp_k_item[m_dimension];
	for (unsigned int i = 0; i < m_dimension; i++) {
		temp_k_item[i] = k_item[i];
	}
	if (quicksort<unsigned int>(temp_k_item, m_dimension, true, true)
			!= m_dimension)
		return false;
	unsigned int hash_code = hashfunc(temp_k_item);
	if (NULL != m_hash_table[hash_code]) {
		m_hash_table[hash_code][m_dimension] += num;
	} else {
		m_hash_table[hash_code] = new unsigned int[m_dimension + 1];
		for (unsigned int i = 0; i < m_dimension; i++) {
			m_hash_table[hash_code][i] = temp_k_item[i];
		}
		m_hash_table[hash_code][m_dimension] = num;
	}
	return true;
}

unsigned int HashTableCounter::get_count(const unsigned int k_item[]) const {
	unsigned int temp_k_item[m_dimension];
	for (unsigned int i = 0; i < m_dimension; i++) {
		temp_k_item[i] = k_item[i];
	}
	if (quicksort<unsigned int>(temp_k_item, m_dimension, true, true)
			!= m_dimension)
		return 0;
	unsigned int hashcode = hashfunc(temp_k_item);
	if (NULL != m_hash_table[hashcode])
		return m_hash_table[hashcode][m_dimension];
	else
		return 0;
}

unsigned int HashTableCounter::hashfunc(const unsigned int k_item[]) const {
	unsigned int hashcode = m_hash_func(k_item, m_dimension, m_table_size);
	if (NULL == m_hash_table[hashcode])
		return hashcode;
	else {
		bool equal = true;
		for (unsigned int i = 0; i < m_dimension; i++) {
			if (m_hash_table[hashcode][i] != k_item[i]) {
				equal = false;
				break;
			}
		}
		if (equal)
			return hashcode;
		else
			return collision_handler(k_item, hashcode);
	}
}

unsigned int HashTableCounter::collision_handler(const unsigned int k_item[],
		unsigned int collision_key) const {
	unsigned int hashcode;
	for (unsigned int i = 1; i < m_table_size; i++) {
//		if (log->isDebugEnabled()) {
//			log->debug("collision_key %u has %u time(s) collision",
//					collision_key, i);
//		}
		hashcode = m_probe_func(k_item, m_dimension, m_table_size,
				collision_key, i);
		if (NULL == m_hash_table[hashcode]) {
			return hashcode;
		} else {
			bool equal = true;
			for (unsigned int j = 0; j < m_dimension; j++) {
				if (m_hash_table[hashcode][j] != k_item[j]) {
					equal = false;
					break;
				}
			}
			if (equal) {
				return hashcode;
			}
		}
	}
	//冲突无法解决
//	if (log->isErrorEnabled()) {
//		log->error("*****collision can't solved!*****");
//	}
	return collision_key;
}
