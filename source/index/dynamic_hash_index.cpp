/*
 * dynamic_hash_index.cpp
 *
 *  Created on: 2013-6-30
 *      Author: Yan Shankai
 */

#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/index/dynamic_hash_index.h"

DynamicHashIndex::DynamicHashIndex(unsigned int bucket_size,
		unsigned int global_deep) {
	m_hash_func = &simple_hash;
	m_bucket_size = bucket_size;
	m_d = global_deep;
	m_catalogs = new Catalog[(int) pow(2, m_d)];
	for (unsigned int i = 0; i < (int) pow(2, m_d); i++) {
		m_catalogs[i].l = m_d;
		m_catalogs[i].bucket = new Bucket();
	}
	m_retry_times = 100;
	m_log_fp = LogUtil::get_instance()->get_log_instance("dynamicHashIndex");
}

DynamicHashIndex::DynamicHashIndex(const DynamicHashIndex& dynamic_hash_index) {
	m_hash_func = dynamic_hash_index.m_hash_func;
	m_bucket_size = dynamic_hash_index.m_bucket_size;
	m_d = dynamic_hash_index.m_d;
	m_retry_times = dynamic_hash_index.m_retry_times;
	m_log_fp = dynamic_hash_index.m_log_fp;
	m_catalogs = new Catalog[(int) pow(2, m_d)];
	for (unsigned int i = 0; i < (int) pow(2, m_d); i++) {
		m_catalogs[i].l = dynamic_hash_index.m_catalogs[i].l;
		m_catalogs[i].bucket = new Bucket();
		vector<IndexHead>& elements =
				dynamic_hash_index.m_catalogs[i].bucket->elements;
		for (unsigned int j = 0; j < elements.size(); j++) {
			IndexHead index_head;
			index_head.identifier =
					new char[strlen(elements[j].identifier) + 1];
			strcpy(index_head.identifier, elements[j].identifier);
			index_head.key_info = new char[strlen(elements[j].key_info) + 1];
			strcpy(index_head.key_info, elements[j].key_info);
			index_head.index_item_num = elements[j].index_item_num;
			index_head.inverted_index = NULL;
			if (elements[j].inverted_index != NULL) {
				index_head.inverted_index = new IndexItem;
				index_head.inverted_index->record_id =
						elements[j].inverted_index->record_id;
				index_head.inverted_index->next = NULL;

				IndexItem* p = elements[j].inverted_index->next;
				IndexItem* q = index_head.inverted_index;
				while (p != NULL) {
					q->next = new IndexItem;
					q = q->next;
					q->record_id = p->record_id;
					q->next = NULL;
					p = p->next;
				}
			}
			m_catalogs[i].bucket->elements.push_back(index_head);
		}
	}
}

DynamicHashIndex::~DynamicHashIndex() {
	unsigned int catalogs_size = pow(2, m_d);
	unsigned char* deleted = new unsigned char[catalogs_size];
	memset(deleted, 0, catalogs_size * sizeof(unsigned char));
	for (unsigned int i = 0; i < catalogs_size; i++) {
		if (deleted[i] == 0) {
			for (vector<IndexHead>::iterator iter =
					m_catalogs[i].bucket->elements.begin();
					iter != m_catalogs[i].bucket->elements.end(); iter++) {
				if (NULL != iter->identifier) {
					delete[] iter->identifier;
				}
				if (NULL != iter->key_info) {
					delete[] iter->key_info;
				}
				IndexItem *p = iter->inverted_index;
				IndexItem *q = NULL;
				while (NULL != p) {
					q = p->next;
					delete p;
					p = q;
				}
			}
			delete m_catalogs[i].bucket;
			for (unsigned int j = 0; j < (int) pow(2, m_d - m_catalogs[i].l);
					j++) {
				deleted[i + j * (int) pow(2, m_catalogs[i].l)] = 1;
			}
		}
	}
	if (m_catalogs != NULL) {
		delete[] m_catalogs;
	}
	delete[] deleted;
}

unsigned int DynamicHashIndex::addressing(unsigned int hashcode) {
	unsigned int mask = 0x01;
	for (unsigned int i = 1; i < m_d; i++) {
		mask |= 0x01 << i;
	}
	return hashcode & mask;
}

pair<unsigned int, int> DynamicHashIndex::locate_index(const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	vector<IndexHead>& elements = m_catalogs[catalog_id].bucket->elements;
	pair<unsigned int, int> result;
	result.first = catalog_id;
	result.second = -1;
	char key_str[key_length + 1];
	strcpy(key_str, key);
	memset(key_str + key_length, '\0', 1 * sizeof(char));
	for (unsigned int i = 0; i < elements.size(); i++) {
		if (strcmp(elements[i].identifier, key_str) == 0) {
			result.second = i;
			break;
		}
	}
	return result;
}

unsigned int DynamicHashIndex::insert(const char *key, size_t key_length,
		char *key_info, unsigned int record_id) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	Bucket* bucket = m_catalogs[catalog_id].bucket;
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

	pair<unsigned int, int> location = locate_index(key, key_length);
	bucket = m_catalogs[location.first].bucket;
	if (location.second == -1) { //不存在此记录
		IndexHead index_head;
		index_head.identifier = new char[key_length + 1];
		strcpy(index_head.identifier, key);
		index_head.key_info = new char[strlen(key_info) + 1];
		strcpy(index_head.key_info, key_info);
		index_head.index_item_num = 1;
		index_head.inverted_index = new IndexItem;
		index_head.inverted_index->record_id = record_id;
		index_head.inverted_index->next = NULL;
		bucket->elements.push_back(index_head);
	} else { //已存在此记录
		IndexItem *p = bucket->elements[location.second].inverted_index;
		IndexItem *q = NULL;
		while (p->record_id > record_id && p->next != NULL) { //降序地插入
			q = p;
			p = p->next;
		}
		if (p->record_id > record_id) {
			bucket->elements[location.second].index_item_num++;
			p->next = new IndexItem;
			p->next->record_id = record_id;
			p->next->next = NULL;
		} else if (p->record_id < record_id) {
			bucket->elements[location.second].index_item_num++;
			if (NULL == q) { //当前链表只有一个记录
				bucket->elements[location.second].inverted_index =
						new IndexItem;
				q = bucket->elements[location.second].inverted_index;
			} else {
				q->next = new IndexItem;
				q = q->next;
			}
			q->record_id = record_id;
			q->next = p;
		}
	}
	return hashcode;
}

bool DynamicHashIndex::split_bucket(unsigned int catalog_id,
		unsigned int local_deep) {
	if (catalog_id > pow(2, m_d) - 1) { //目录索引越界
		return false;
	}
	if (m_d == m_catalogs[catalog_id].l) { //局部深度与全局深度相等
		return false;
	}
	Bucket* old_bucket = m_catalogs[catalog_id].bucket;
	if (local_deep == 0) { //默认分裂方式
		//调整目录的桶指向
		unsigned int old_deep = m_catalogs[catalog_id].l;
		for (unsigned int i = 0; i < 2; i++) {
			Bucket* new_bucket = new Bucket();
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
		for (unsigned int i = 0; i < (int) pow(2, local_deep - old_deep); i++) {
			Bucket* new_bucket = new Bucket();
			for (unsigned int j = 0; j < (int) pow(2, m_d - local_deep); j++) {
				unsigned int cid = (catalog_id + i * (int) pow(2, old_deep)
						+ j * (int) pow(2, local_deep)) % (int) pow(2, m_d);
				m_catalogs[cid].bucket = new_bucket;
				m_catalogs[cid].l = local_deep;
			}
		}
	}
	//重新分配旧桶的元素
	for (vector<IndexHead>::iterator iter = old_bucket->elements.begin();
			iter != old_bucket->elements.end(); iter++) {
		m_catalogs[addressing(
				hashfunc(iter->identifier, strlen(iter->identifier)))].bucket->elements.push_back(
				*iter);
	}
	delete old_bucket;
	return true;
}

bool DynamicHashIndex::split_catalog(unsigned int global_deep) {
	unsigned int old_d = m_d;
	Catalog* old_catalogs = m_catalogs;
	if (global_deep == 0) { //默认分裂方式
		m_d++;
		//目录分裂成2倍
		m_catalogs = new Catalog[(int) pow(2, m_d)];
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
		m_catalogs = new Catalog[(int) pow(2, m_d)];
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

unsigned int DynamicHashIndex::size_of_index() {
	unsigned int result = 0;
	unsigned int catalogs_size = pow(2, m_d);
	result += catalogs_size * sizeof(Catalog); //计算目录的大小
	unsigned char calculated[catalogs_size];
	memset(calculated, 0, catalogs_size * sizeof(unsigned char));
	unsigned int dynamic_char_num = 0;
	unsigned int dynamic_item_num = 0;
	for (unsigned int i = 0; i < catalogs_size; i++) {
		if (calculated[i] == 0) {
			result += sizeof(*m_catalogs[i].bucket); //计算每个桶（及其包含的索引头结构）的大小
			for (vector<IndexHead>::iterator iter =
					m_catalogs[i].bucket->elements.begin();
					iter != m_catalogs[i].bucket->elements.end(); iter++) { //计算每个索引头里的动态数据大小以及索引项大小
				if (NULL != iter->identifier) {
					dynamic_char_num += strlen(iter->identifier) + 1;
				}
				if (NULL != iter->key_info) {
					dynamic_char_num += strlen(iter->key_info) + 1;
				}
				dynamic_item_num += iter->index_item_num;
			}
			for (unsigned int j = 0; j < (int) pow(2, m_d - m_catalogs[i].l);
					j++) {
				calculated[i + j * (int) pow(2, m_catalogs[i].l)] = 1;
			}
		}
	}
	result += dynamic_char_num * sizeof(char)
			+ dynamic_item_num * sizeof(IndexItem);
	return result;
}

unsigned int DynamicHashIndex::get_mark_record_num(const char *key,
		size_t key_length) {
	pair<unsigned int, int> location = locate_index(key, key_length);
	if (location.second == -1) {
		return 0;
	} else {
		return m_catalogs[location.first].bucket->elements[location.second].index_item_num;
	}
}

unsigned int DynamicHashIndex::get_real_record_num(const char *key,
		size_t key_length) {
	pair<unsigned int, int> location = locate_index(key, key_length);
	if (location.second == -1) {
		return 0;
	} else {
		unsigned int count = 0;
		IndexItem *p =
				m_catalogs[location.first].bucket->elements[location.second].inverted_index;
		while (p != NULL) {
			count++;
			p = p->next;
		}
		return count;
	}
}

unsigned int DynamicHashIndex::find_record(unsigned int *records,
		const char *key, size_t key_length) {
	pair<unsigned int, int> location = locate_index(key, key_length);
	if (location.second == -1) {
		return 0;
	} else {
		IndexHead& index_head =
				m_catalogs[location.first].bucket->elements[location.second];
		IndexItem *p = index_head.inverted_index;
		unsigned int i = 0;
		for (i = 0; i < index_head.index_item_num; i++) {
			if (NULL == p) {
				break;
			} else {
				records[i] = p->record_id;
				p = p->next;
			}
		}
		if (NULL == p && i == index_head.index_item_num) {
			return index_head.index_item_num;
		} else if (i < index_head.index_item_num) { //计数值大于实际值
			return i;
		} else { //计数值小于实际值
			return get_real_record_num(key, key_length);
		}
	}
}

bool DynamicHashIndex::get_key_info(char **key_info, const char *key,
		size_t key_length) {
	pair<unsigned int, int> location = locate_index(key, key_length);
	if (location.second == -1) {
		return false;
	} else {
		*key_info =
				new char[strlen(
						m_catalogs[location.first].bucket->elements[location.second].key_info)
						+ 1];
		strcpy(*key_info,
				m_catalogs[location.first].bucket->elements[location.second].key_info);
		return true;
	}
}

unsigned int* DynamicHashIndex::get_intersect_records(const char **keys,
		unsigned int key_num) {
	if (keys == NULL) {
		return NULL;
	}
	if (key_num == 1) {
		unsigned int record_num = get_mark_record_num(keys[0], strlen(keys[0]));
		unsigned int *records = new unsigned int[1 + record_num];
		records[0] = record_num;
		find_record(records + 1, keys[0], strlen(keys[0]));
		return records;
	}
	if (key_num > 1) {
		IndexItem **ptr = new IndexItem*[key_num];
		IndexItem *cur_min = NULL;
		unsigned int intersect_num = 0;
		unsigned int records_max_num = 0;
		unsigned int ptr_num = key_num;
		unsigned int hashcode;
		//准备纸带针孔，根据keys的每个值(i)来初始化跟踪指针ptr[j]，并记录针对准的孔中最小的一个cur_min和最少的记录数records_max_num，交集大小不会超过records_max_num
		for (unsigned int i = 0, j = 0; i < key_num; i++) {
			pair<unsigned int, int> location = locate_index(keys[i],
					strlen(keys[i]));
			IndexHead& index_head =
					m_catalogs[location.first].bucket->elements[location.second];
			if (location.second == -1) {
				ptr_num--;
				continue;
			} else {
				if (j == 0) {
					cur_min = index_head.inverted_index;
					records_max_num = index_head.index_item_num;
				} else {
					if (index_head.inverted_index->record_id
							< cur_min->record_id) {
						cur_min = index_head.inverted_index;
					}
					if (index_head.index_item_num < records_max_num) {
						records_max_num = index_head.index_item_num;
					}
				}
				ptr[j] = index_head.inverted_index;
				j++;
			}
		}

		unsigned int *records = new unsigned int[records_max_num + 1];
		bool finished = false;
		bool intersect = true;
		//如果找到的索引项小于2个则视为交集操作返回空
		if (ptr_num < 2) {
			finished = true;
		}
		/* 开始穿孔 */
		while (!finished) {
			intersect = true;
			for (unsigned int i = 0; i < ptr_num; i++) {
				if (ptr[i] == cur_min) {
					continue;
				}
				if (ptr[i]->record_id > cur_min->record_id) {
					//对齐当前纸带的针孔
					do {
						ptr[i] = ptr[i]->next;
					} while (ptr[i] != NULL
							&& ptr[i]->record_id > cur_min->record_id);
				}
				if (ptr[i] == NULL) {
					finished = true;
					intersect = false;
					break;
				}
				if (ptr[i]->record_id < cur_min->record_id) { //cur_min的record_id不在交集内
					cur_min = ptr[i];
					intersect = false;
					break;
				}
			}
			if (intersect) {
				records[++intersect_num] = cur_min->record_id;
				for (unsigned int i = 0; i < ptr_num; i++) {
					ptr[i] = ptr[i]->next;
					if (NULL == ptr[i]) {
						finished = true;
					}
				}
				if (!finished) {
					cur_min = min_record_id(ptr, ptr_num);
				}
			}
		}
		/* 结束穿孔 */
		records[0] = intersect_num;
		delete[] ptr;
		return records;
	} else {
		unsigned int *records = new unsigned int[1];
		records[0] = 0;
		return records;
	}
}

bool DynamicHashIndex::change_key_info(const char *key, size_t key_length,
		const char* key_info) {
	pair<unsigned int, int> location = locate_index(key, key_length);
	if (location.second == -1) {
		return false;
	} else {
		if (m_catalogs[location.first].bucket->elements[location.second].key_info
				!= NULL) {
			delete[] m_catalogs[location.first].bucket->elements[location.second].key_info;
		}
		m_catalogs[location.first].bucket->elements[location.second].key_info =
				new char[strlen(key_info) + 1];
		strcpy(
				m_catalogs[location.first].bucket->elements[location.second].key_info,
				key_info);
		return true;
	}
}

unsigned int DynamicHashIndex::hashfunc(const char *str, size_t length) {
	return m_hash_func(str, length, UINT_MAX);
}

#ifdef __DEBUG__
void DynamicHashIndex::print_index(const char** identifiers) {
	const char* identifier = NULL;
	DynamicHashIndex& index = *this;
	Catalog* catalogs = (Catalog*) index.get_hash_table();
	for (unsigned int i = 0; i < (unsigned int) pow(2, index.get_global_deep());
			i++) {
		if (catalogs[i].bucket != NULL) {
			vector<IndexHead>& elements = catalogs[i].bucket->elements;
			for (unsigned int j = 0; j < elements.size(); j++) {
				if (identifiers != NULL) {
					identifier = identifiers[ysk_atoi(elements[j].key_info,
							strlen(elements[j].key_info))];
				} else {
					identifier = elements[j].identifier;
				}
				printf(
						"catalog: %u\thashcode: %u\tkey: %s------Record numbers: %u------Record index: ",
						i, index.hashfunc(identifier, strlen(identifier)),
						identifier, elements[j].index_item_num);
				unsigned int records[index.get_mark_record_num(identifier,
						strlen(identifier))];
				unsigned int num = index.find_record(records, identifier,
						strlen(identifier));
				for (unsigned int j = 0; j < num; j++) {
					printf("%u, ", records[j]);
				}

				printf("\n");
			}
		}
	}
}
#endif
