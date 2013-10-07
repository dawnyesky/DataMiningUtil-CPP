/*
 * ro_dynamic_hash_index.cpp
 *
 *  Created on: 2013-10-4
 *      Author: Yan Shankai
 */

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <limits.h>
#include <numeric>
#include "libyskdmu/index/dynamic_hash_index.h"
#include "libyskdmu/index/ro_dynamic_hash_index.h"

using namespace std;

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

RODynamicHashIndex::~RODynamicHashIndex() {
	if (m_data != NULL) {
		free(m_data);
	}
	if (m_l1_index != NULL) {
		delete m_l1_index;
	}
	if (m_l2_index != NULL) {
		free(m_l2_index);
	}
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

bool RODynamicHashIndex::build(HashIndex* original_index) {
	DynamicHashIndex& index = *((DynamicHashIndex*) original_index);
	m_hash_func = index.get_hash_func();
	m_d = index.m_d;

	unsigned int catalogs_size = pow(2, index.m_d);
	unsigned int cat_bucket_index[catalogs_size];
	unsigned char set[catalogs_size]; //标记该目录指向的桶是否已经设置过
	memset(set, 0, catalogs_size * sizeof(unsigned char));
	vector<unsigned int> index_head_nums; //每个桶的索引头数量
	vector<char*> identifiers; //所有索引标识的指针
	vector<unsigned int> index_nums; //所有索引头所带的索引数量
	vector<IndexItem*> index_list; //所有索引头所带的索引

	//遍历所有索引把信息先放到临时空间
	for (unsigned int i = 0; i < catalogs_size; i++) {
//		printf("set[%u]:%u\n", i, set[i]);
		if (set[i] == 0) {
//			printf("Here:%u\n", catalogs_size);
			index_head_nums.push_back(
					index.m_catalogs[i].bucket->elements.size());
			for (vector<IndexHead>::iterator iter =
					index.m_catalogs[i].bucket->elements.begin();
					iter != index.m_catalogs[i].bucket->elements.end();
					iter++) {
				identifiers.push_back(iter->identifier);
				index_nums.push_back(iter->index_item_num);
				index_list.push_back(iter->inverted_index);
			}
			for (unsigned int j = 0;
					j < (int) pow(2, index.m_d - index.m_catalogs[i].l); j++) {
				unsigned int cid = i + j * (int) pow(2, index.m_catalogs[i].l);
				cat_bucket_index[cid] = index_head_nums.size() - 1;
//				printf("set %u with %u\n", cid, index_head_nums.size() - 1);
				set[cid] = 1;
			}
		}
	}

//	printf("index_head_nums size:%u\n", index_head_nums.size());
//	printf("identifiers size:%u\n", identifiers.size());
//	printf("index_nums size:%u\n", index_nums.size());
//	printf("index_list size:%u\n", index_list.size());

	//构建索引数据区
	m_data_size = accumulate(index_nums.begin(), index_nums.end(), 0)
			+ index_nums.size();
	unsigned int* data = (unsigned int*) malloc(
			m_data_size * sizeof(unsigned int));
	IndexItem *p, *q;
	unsigned int total_offset = 0; //数据区的偏移
	unsigned int index_offset[index_nums.size()]; //每个索引头在数据区的偏移
	data[0] = index_nums[0];
	index_offset[0] = 0;
	p = index_list[0];
	q = NULL;
	while (NULL != p) {
		q = p->next;
		data[++total_offset] = p->record_id;
		p = q;
	}
	for (unsigned int i = 0; i < index_nums.size() - 1; i++) {
		data[++total_offset] = index_nums[i + 1];
		index_offset[i + 1] = total_offset;
		p = index_list[i + 1];
		q = NULL;
		while (NULL != p) {
			q = p->next;
			data[++total_offset] = p->record_id;
			p = q;
		}
	}
	m_data = data;

	//构建索引数据区的二级索引
	m_l2_index_size = 0;
	m_l2_index_size += (2 * identifiers.size() + index_head_nums.size())
			* sizeof(unsigned int); //每个标识的长度和索引头偏移+每个桶的元素个数
	unsigned int total_char_num = 0;
	for (unsigned int i = 0; i < identifiers.size(); i++) {
		total_char_num += strlen(identifiers[i]) + 1; //把结束符也拷贝进去
	}
	m_l2_index_size += total_char_num * sizeof(char);
	m_l2_index = malloc(m_l2_index_size);
	unsigned int buckets_offset[index_head_nums.size()];
	for (unsigned int i = 0, id_index = 0, data_offset = 0;
			i < index_head_nums.size(); i++) {
		buckets_offset[i] = data_offset;
		memcpy(m_l2_index + data_offset, &index_head_nums[i],
				sizeof(unsigned int));
		data_offset += sizeof(unsigned int);
		for (unsigned int j = 0; j < index_head_nums[i]; j++) {
			unsigned int id_len = strlen(identifiers[id_index]) + 1;
			memcpy(m_l2_index + data_offset, &id_len, sizeof(unsigned int));
			data_offset += sizeof(unsigned int);
			memcpy(m_l2_index + data_offset, identifiers[id_index],
					id_len * sizeof(char));
			data_offset += id_len * sizeof(char);
			memcpy(m_l2_index + data_offset, index_offset + id_index,
					sizeof(unsigned int));
			data_offset += sizeof(unsigned int);
			id_index++;
		}
	}

	//构建索引数据区的一级索引
	m_l1_index_size = catalogs_size;
	m_l1_index = new unsigned int[catalogs_size];
	memset(set, 0, catalogs_size * sizeof(unsigned char)); //重用set数组
	for (unsigned int i = 0; i < catalogs_size; i++) {
		if (set[i] == 0) {
			for (unsigned int j = 0;
					j < (int) pow(2, index.m_d - index.m_catalogs[i].l); j++) {
				unsigned int cid = i + j * (int) pow(2, index.m_catalogs[i].l);
				m_l1_index[cid] = buckets_offset[cat_bucket_index[cid]];
				set[cid] = 1;
			}
		}
	}

//	print_l1_index();
//	print_l2_index();
//	print_index_data();
//	printf("cat_bucket_index:");
//	for (unsigned int i = 0; i < catalogs_size; i++) {
//		printf("%u, ", cat_bucket_index[i]);
//	}
//	printf("\n");
//	printf("buckets_offset:");
//	for (unsigned int i = 0; i < index_head_nums.size(); i++) {
//		printf("%u, ", buckets_offset[i]);
//	}
//	printf("\n");

	m_is_built = true;
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
		for (int i = 1; i < key_num; i++) {
			intersect_num = 0;
			for (int m = 1, n = 0; m < (record_num + 1) && n < record_nums[i];
					) {
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
				}
			}
			memcpy(result + 1, result_tmp,
					sizeof(unsigned int) * intersect_num);
			record_num = intersect_num;
		}
		result[0] = record_num; //结果第一位放交集元素个数
		return result;
	}
	return NULL;
}

bool RODynamicHashIndex::offload_data() {
	unsigned int* index_data = (unsigned int*) m_data;
#pragma offload target(mic)\
		in(m_d:alloc_if(1) free_if(0))\
		in(index_data:length(m_data_size) alloc_if(1) free_if(0))\
		in(m_data_size:alloc_if(1) free_if(0))\
		in(m_l1_index:length(m_l1_index_size) alloc_if(1) free_if(0))\
		in(m_l1_index_size:alloc_if(1) free_if(0))\
		in(m_l2_index:length(m_l2_index_size) alloc_if(1) free_if(0))\
		in(m_l2_index_size:alloc_if(1) free_if(0)\
		out(m_accard_inst:alloc_if(1) free_if(0))
	{
		m_accard_inst = new RODynamicHashIndex();
		m_accard_inst->m_accard_inst = this;
		m_accard_inst->m_d = m_d;
		m_accard_inst->m_data = m_data;
		m_accard_inst->m_data_size = m_data_size;
		m_accard_inst->m_l1_index = m_l1_index;
		m_accard_inst->m_l1_index_size = m_l1_index_size;
		m_accard_inst->m_l2_index = m_l2_index;
		m_accard_inst->m_l2_index_size = m_l2_index_size;
	}
	return true;
}

bool RODynamicHashIndex::recycle_data() {
	unsigned int* index_data = (unsigned int*) m_data;
#pragma offload target(mic)\
		nocopy(m_d:alloc_if(1) free_if(0))\
		nocopy(index_data:length(m_data_size) alloc_if(0) free_if(1))\
		nocopy(m_data_size:alloc_if(0) free_if(1))\
		nocopy(m_l1_index:length(m_l1_index_size) alloc_if(0) free_if(1))\
		nocopy(m_l1_index_size:alloc_if(0) free_if(1))\
		nocopy(m_l2_index:length(m_l2_index_size) alloc_if(0) free_if(1))\
		nocopy(m_l2_index_size:alloc_if(0) free_if(1))\
		out(m_accard_inst:alloc_if(0) free_if(1))
	{
		delete m_accard_inst;
		m_accard_inst = NULL;
	}
	return true;
}

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
