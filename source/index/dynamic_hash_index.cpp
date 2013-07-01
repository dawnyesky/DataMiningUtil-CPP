/*
 * dynamic_hash_index.cpp
 *
 *  Created on: 2013-6-30
 *      Author: Yan Shankai
 */

#include "libyskdmu/index/dynamic_hash_index.h"

DynamicHashIndex::DynamicHashIndex(unsigned int bucket_size,
		unsigned int global_deep) {
	m_bucket_size = bucket_size;
	m_d = global_deep;
}

DynamicHashIndex::DynamicHashIndex(const DynamicHashIndex& dynamic_hash_index) {

}

DynamicHashIndex::~DynamicHashIndex() {

}

bool DynamicHashIndex::init(HashFunc hash_func) {
	m_hash_func = hash_func;
	return true;
}

unsigned int DynamicHashIndex::insert(const char *key, size_t key_length,
		unsigned int& key_info, unsigned int record_id) {
	return 0;
}

unsigned int DynamicHashIndex::size_of_index() {
	return 0;
}

unsigned int DynamicHashIndex::get_mark_record_num(const char *key,
		size_t key_length) {
	return 0;
}

unsigned int DynamicHashIndex::get_real_record_num(const char *key,
		size_t key_length) {
	return 0;
}

unsigned int DynamicHashIndex::find_record(unsigned int *records, const char *key,
		size_t key_length) {
	return 0;
}

bool DynamicHashIndex::get_key_info(unsigned int& key_info, const char *key,
		size_t key_length) {
	return false;
}

unsigned int* DynamicHashIndex::get_intersect_records(const char **keys,
		unsigned int key_num) {
	return NULL;
}

unsigned int DynamicHashIndex::hashfunc(const char *str, size_t length) {
	return 0;
}
