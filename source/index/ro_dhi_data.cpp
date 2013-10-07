/*
 * ro_dhi_data.cpp
 *
 *  Created on: 2013-10-11
 *      Author: Yan Shankai
 */

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <numeric>
#include "libyskdmu/index/dynamic_hash_index.h"
#include "libyskdmu/index/ro_dhi_data.h"

RODynamicHashIndexData::RODynamicHashIndexData() {
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

RODynamicHashIndexData::~RODynamicHashIndexData() {
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

bool RODynamicHashIndexData::fill_memeber_data(unsigned int** deep,
		unsigned int** data, unsigned int** data_size, unsigned int** l1_index,
		unsigned int** l1_index_size, unsigned char** l2_index,
		unsigned int** l2_index_size) {
	if (!m_is_built) {
		return false;
	}
	*deep = &m_d;
	*data = (unsigned int*) m_data;
	*data_size = &m_data_size;
	*l1_index = m_l1_index;
	*l1_index_size = &m_l1_index_size;
	*l2_index = (unsigned char*) m_l2_index;
	*l2_index_size = &m_l2_index_size;
	return true;
}

bool RODynamicHashIndexData::build(HashIndex* original_index) {
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
