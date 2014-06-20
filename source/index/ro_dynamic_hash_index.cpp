/*
 * ro_dynamic_hash_index.cpp
 *
 *  Created on: 2013-10-4
 *      Author: Yan Shankai
 */

#ifdef OMP
#pragma offload_attribute (push, target(mic))
#endif

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "libyskdmu/index/ro_dynamic_hash_index.h"

#ifdef OMP
#pragma offload_attribute (pop)
#endif

unsigned int simple_hash_mic(const char *key, unsigned int length,
		unsigned int table_size) {
	unsigned int hashcode = 0;
	for (unsigned int i = 0; i < length; i++) {
		hashcode = 37 * hashcode + key[i];
	}
	return hashcode %= table_size;
}

RODynamicHashIndex::RODynamicHashIndex() {
	m_d = 0;
	m_data = NULL;
	m_data_size = 0;
	m_hash_func = NULL;
	m_l1_index = NULL;
	m_l1_index_size = 0;
	m_l2_index = NULL;
	m_l2_index_size = 0;
	m_is_built = false;
}

RODynamicHashIndex::RODynamicHashIndex(unsigned int* deep, unsigned int* data,
		unsigned int* data_size, unsigned int* l1_index,
		unsigned int* l1_index_size, void* l2_index,
		unsigned int* l2_index_size, HashFunc hash_func) {
	m_d = *deep;
	m_data = data;
	m_data_size = *data_size;
	m_l1_index = l1_index;
	m_l1_index_size = *l1_index_size;
	m_l2_index = l2_index;
	m_l2_index_size = *l2_index_size;
	m_hash_func = hash_func;
	m_is_built = true;
}

RODynamicHashIndex::~RODynamicHashIndex() {
	//数据是外部存储的，不应该随对象的销毁而回收空间
}

unsigned int RODynamicHashIndex::addressing(unsigned int hashcode) {
	unsigned int mask = 0x01;
	for (unsigned int i = 1; i < m_d; i++) {
		mask |= 0x01 << i;
	}
	return hashcode & mask;
}

pair<unsigned int, void*> RODynamicHashIndex::locate_index(const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	void* l2_index = m_l2_index + m_l1_index[catalog_id];
//	printf("cid:%u, l2_index_offset:%u\n", catalog_id, m_l1_index[catalog_id]);
	unsigned int elements_num = 0;
	memcpy(&elements_num, l2_index, sizeof(unsigned int));
	l2_index += sizeof(unsigned int);
	pair<unsigned int, void*> result;
	result.first = catalog_id;
	result.second = NULL;
	char key_str[key_length + 1];
	strcpy(key_str, key);
	memset(key_str + key_length, '\0', 1 * sizeof(char));
	char* identifer;
	for (unsigned int i = 0; i < elements_num; i++) {
		unsigned int id_len = 0;
		memcpy(&id_len, l2_index, sizeof(unsigned int));
		l2_index += sizeof(unsigned int);
		identifer = (char*) l2_index;
		l2_index += id_len * sizeof(char);
		if (strcmp(identifer, key_str) == 0) {
			unsigned int index_offset = 0;
			memcpy(&index_offset, l2_index, sizeof(unsigned int));
//			printf("found at element %u, index_data_offset:%u\n", i,
//					index_offset);
			result.second = m_data + index_offset * sizeof(unsigned int);
			break;
		}
		l2_index += 1 * sizeof(unsigned int);
	}
	return result;
}

bool RODynamicHashIndex::fill_memeber_data(unsigned int** deep,
		unsigned int** data, unsigned int** data_size, unsigned int** l1_index,
		unsigned int** l1_index_size, void** l2_index,
		unsigned int** l2_index_size) {
	if (!m_is_built) {
		return false;
	}
	*deep = &m_d;
	*data = (unsigned int*) m_data;
	*data_size = &m_data_size;
	*l1_index = m_l1_index;
	*l1_index_size = &m_l1_index_size;
	*l2_index = m_l2_index;
	*l2_index_size = &m_l2_index_size;
	return true;
}

unsigned int RODynamicHashIndex::get_record_num(const char *key,
		size_t key_length) {
	pair<unsigned int, void*> location = locate_index(key, key_length);
	if (location.second == NULL) {
		return 0;
	} else {
		unsigned int result = 0;
		memcpy(&result, location.second, sizeof(unsigned int));
		return result;
	}
}

unsigned int RODynamicHashIndex::find_record(unsigned int *records,
		const char *key, size_t key_length) {
	pair<unsigned int, void*> location = locate_index(key, key_length);
	if (location.second == NULL) {
		return 0;
	} else {
		unsigned int index_item_num = 0;
		memcpy(&index_item_num, location.second, sizeof(unsigned int));
		memcpy(records, location.second + sizeof(unsigned int),
				index_item_num * sizeof(unsigned int));
		return index_item_num;
	}
}

unsigned int* RODynamicHashIndex::get_intersect_records(const char **keys,
		unsigned int key_num) {
	if (keys == NULL) {
		return NULL;
	}
	if (key_num == 1) {
		unsigned int record_num = get_record_num(keys[0], strlen(keys[0]));
		unsigned int *records = new unsigned int[1 + record_num];
		records[0] = record_num;
		find_record(records + 1, keys[0], strlen(keys[0]));
		return records;
	}
	if (key_num > 1) {
		//准备索引数据
		unsigned int* records[key_num];
		unsigned int record_nums[key_num];
		for (unsigned int i = 0; i < key_num; i++) {
			pair<unsigned int, void*> location = locate_index(keys[i],
					strlen(keys[i]));
			if (location.second == NULL) {
				unsigned int* result = new unsigned int;
				*result = 0;
				return result;
			}
			memcpy(record_nums + i, location.second, sizeof(unsigned int));
			records[i] =
					(unsigned int*) (location.second + sizeof(unsigned int));
		}

		//求出records中key_num个数组的交集
		unsigned int intersect_num = 0; //交集的元素个数
		unsigned int *result = new unsigned int[record_nums[0] + 1]; //结果
		unsigned int *result_tmp = new unsigned int[record_nums[0]];
		unsigned int record_num = record_nums[0];
		memcpy(result + 1, records[0], sizeof(unsigned int) * (record_nums[0]));
		for (unsigned int i = 1; i < key_num; i++) {
			intersect_num = 0;
			for (unsigned int m = 1, n = 0;
					m < (record_num + 1) && n < record_nums[i];) {
				/* 原始逻辑代码
				if (records[i][n] > result[m]) {
					n++;
					continue;
				}
				if (records[i][n] < result[m]) {
					m++;
					continue;
				}
				if (records[i][n] == result[m]) {
					result_tmp[intersect_num++] = result[m];
					n++;
					m++;
				} */
				/* 去分支代码 */
				result_tmp[intersect_num] = result[m];
				int diff = records[i][n] - result[m];
				m += ((diff >> 31) & 1) + ((~(diff | -diff) >> 31) & 1);
				n += ((-diff >> 31) & 1) + ((~(diff | -diff) >> 31) & 1);
				intersect_num += ((~(diff | -diff) >> 31) & 1);
			}
			memcpy(result + 1, result_tmp,
					sizeof(unsigned int) * intersect_num);
			record_num = intersect_num;
		}
		result[0] = record_num; //结果第一位放交集元素个数
		delete[] result_tmp;
		return result;
	}
	return NULL;
}

#ifdef OMP
bool RODynamicHashIndex::offload_data() {
	unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
	*l2_index_size;
	void* l2_index;
	fill_memeber_data(&d, &data, &data_size, &l1_index, &l1_index_size,
			&l2_index, &l2_index_size);
#pragma offload target(mic)\
		in(d:length(1) alloc_if(1) free_if(0))\
		in(data:length(*data_size) alloc_if(1) free_if(0))\
		in(data_size:length(1) alloc_if(1) free_if(0))\
		in(l1_index:length(*l1_index_size) alloc_if(1) free_if(0))\
		in(l1_index_size:length(1) alloc_if(1) free_if(0))\
		in(l2_index:length(*l2_index_size) alloc_if(1) free_if(0))\
		in(l2_index_size:length(1) alloc_if(1) free_if(0))
	{
	}
	return true;
}

bool RODynamicHashIndex::recycle_data() {
	unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
	*l2_index_size;
	void* l2_index;
	fill_memeber_data(&d, &data, &data_size, &l1_index, &l1_index_size,
			&l2_index, &l2_index_size);
#pragma offload target(mic)\
		nocopy(d:length(1) alloc_if(0) free_if(1))\
		nocopy(data:length(*data_size) alloc_if(0) free_if(1))\
		nocopy(data_size:length(1) alloc_if(0) free_if(1))\
		nocopy(l1_index:length(*l1_index_size) alloc_if(0) free_if(1))\
		nocopy(l1_index_size:length(1) alloc_if(0) free_if(1))\
		nocopy(l2_index:length(*l2_index_size) alloc_if(0) free_if(1))\
		nocopy(l2_index_size:length(1) alloc_if(0) free_if(1))
	{
	}
	return true;
}
#endif

unsigned int RODynamicHashIndex::hashfunc(const char *str, size_t length) {
	return m_hash_func(str, length, UINT_MAX);
}

void RODynamicHashIndex::print_l1_index() {
	if (m_l1_index_size == 0) {
		printf("L1 index is empty!\n");
	}
	printf("****L1 index(%u) start****\n", m_l1_index_size);
	for (unsigned int i = 0; i < m_l1_index_size; i++) {
		printf("%u ", m_l1_index[i]);
	}
	printf("\n");
	printf("*******L1 index end*******\n");
}

void RODynamicHashIndex::print_l2_index() {
	if (m_l2_index_size == 0) {
		printf("L2 index is empty!\n");
	}
	printf("****L2 index(%u) start****\n", m_l2_index_size);
	printf("---{id_len|identifier|index_offset}---\n");
	unsigned int l2_offset = 0;
	unsigned int bucket_id = 0;
	while (l2_offset < m_l2_index_size) {
		unsigned int element_num = 0;
		memcpy(&element_num, m_l2_index + l2_offset, sizeof(unsigned int));
		printf("bucket id:%u, element num:%u, l2_offset:%u\n", bucket_id,
				element_num, l2_offset);
		l2_offset += sizeof(unsigned int);
		for (unsigned int i = 0; i < element_num; i++) {
			unsigned int id_len = 0;
			memcpy(&id_len, m_l2_index + l2_offset, sizeof(unsigned int));
			l2_offset += sizeof(unsigned int);
			printf("{%u|", id_len);
			char id[id_len];
			memset(id, 0, id_len);
			memcpy(id, m_l2_index + l2_offset, id_len * sizeof(char));
			l2_offset += id_len * sizeof(char);
			printf("%s|", id);
			unsigned int offset = 0;
			memcpy(&offset, m_l2_index + l2_offset, sizeof(unsigned int));
			l2_offset += sizeof(unsigned int);
			printf("%u}  ", offset);
		}
		printf("\n");
		bucket_id++;
	}
	printf("*******L2 index end*******\n");
}

void RODynamicHashIndex::print_index_data() {
	if (m_data_size == 0) {
		printf("Index is empty!\n");
	}
	printf("****Index(%u) start****\n", m_data_size);
	printf("---{index_offset:index_item_num|item1 [ itemn...]}---\n");
	unsigned int item_num = 0;
	for (unsigned int i = 0; i < m_data_size; i++) {
		if (item_num == 0) {
			item_num = ((unsigned int*) m_data)[i];
			printf("{ %u:%u|", i, item_num);
		} else {
			printf("%u ", ((unsigned int*) m_data)[i]);
			item_num--;
		}
		if (item_num == 0) {
			printf("}  ");
		}
	}
	printf("\n");
	printf("*******Index end*******\n");
}
