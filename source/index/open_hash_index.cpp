/*
 * open_hash_index.cpp
 *
 *  Created on: 2013-6-30
 *      Author: Yan Shankai
 */

#include <string.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/index/open_hash_index.h"

unsigned int probe(const char *key, unsigned int length,
		unsigned int table_size, unsigned int collision_key,
		unsigned int probe_step) {
	return (collision_key + probe_step * probe_step) % table_size;
}

OpenHashIndex::OpenHashIndex() {
	m_table_size = 1000;
	m_hash_table = new IndexHead*[m_table_size];
	for (unsigned int i = 0; i < m_table_size; i++)
		m_hash_table[i] = NULL;
	m_hash_func = &simple_hash;
	m_probe_func = &probe;
	m_log_fp = LogUtil::get_instance()->get_log_instance("openHashIndex");
}

OpenHashIndex::OpenHashIndex(unsigned int table_size, HashFunc hash_func,
		ProbeFunc probe_func) {
	m_table_size = table_size;
	if (0 == m_table_size)
		m_table_size = 100;
	if (m_table_size > MAX_TABLE_SIZE) {
		m_table_size = MAX_TABLE_SIZE;
	}
	m_hash_table = new IndexHead*[m_table_size];
	for (unsigned int i = 0; i < m_table_size; i++)
		m_hash_table[i] = NULL;
	m_hash_func = hash_func;
	m_probe_func = probe_func;
	m_log_fp = LogUtil::get_instance()->get_log_instance("openHashIndex");
}

OpenHashIndex::OpenHashIndex(const OpenHashIndex& hash_index) {
	m_table_size = hash_index.m_table_size;
	if (m_table_size > MAX_TABLE_SIZE) {
		m_table_size = MAX_TABLE_SIZE;
	}
	m_hash_table = new IndexHead*[hash_index.m_table_size];
	for (unsigned int i = 0; i < hash_index.m_table_size; i++) {
		if (NULL != hash_index.m_hash_table[i]) {
			m_hash_table[i] = new IndexHead;
			size_t id_len = strlen(hash_index.m_hash_table[i]->identifier);
			if (id_len > 0) {
				m_hash_table[i]->identifier = new char[id_len + 1];
				strcpy(m_hash_table[i]->identifier,
						hash_index.m_hash_table[i]->identifier);
			}
			size_t info_len = strlen(hash_index.m_hash_table[i]->key_info);
			if (info_len > 0) {
				m_hash_table[i]->key_info = new char[info_len + 1];
				strcpy(m_hash_table[i]->key_info,
						hash_index.m_hash_table[i]->key_info);
			}
			m_hash_table[i]->index_item_num =
					hash_index.m_hash_table[i]->index_item_num;
			IndexItem *p = hash_index.m_hash_table[i]->inverted_index;
			IndexItem *q = NULL;
			if (NULL != p) {
				m_hash_table[i]->inverted_index = new IndexItem();
				q = m_hash_table[i]->inverted_index;
				q->record_id = p->record_id;
				q->next = NULL;
				p = p->next;
				while (NULL != p) {
					q->next = new IndexItem();
					q = q->next;
					q->record_id = p->record_id;
					q->next = NULL;
					p = p->next;
				}
			}
		} else {
			m_hash_table[i] = NULL;
		}
	}
	m_hash_func = hash_index.m_hash_func;
	m_probe_func = hash_index.m_probe_func;
	m_log_fp = hash_index.m_log_fp;
}

OpenHashIndex::~OpenHashIndex() {
	for (unsigned int i = 0; i < m_table_size; i++) {
		if (NULL != m_hash_table[i]) {
			if (NULL != m_hash_table[i]->identifier) {
				delete[] m_hash_table[i]->identifier;
			}
			if (NULL != m_hash_table[i]->key_info) {
				delete[] m_hash_table[i]->key_info;
			}
			IndexItem *p = m_hash_table[i]->inverted_index;
			IndexItem *q = NULL;
			while (NULL != p) {
				q = p->next;
				delete p;
				p = q;
			}
			delete m_hash_table[i];
		}
	}
	delete[] m_hash_table;
}

unsigned int OpenHashIndex::insert(const char *key, size_t key_length,
		char *key_info, unsigned int record_id) {
	unsigned int hashcode = hashfunc(key, key_length);
	if (NULL == m_hash_table[hashcode]) {
		m_hash_table[hashcode] = new IndexHead;
		m_hash_table[hashcode]->identifier = new char[key_length + 1];
		strcpy(m_hash_table[hashcode]->identifier, key);
		m_hash_table[hashcode]->key_info = new char[strlen(key_info) + 1];
		strcpy(m_hash_table[hashcode]->key_info, key_info);
		m_hash_table[hashcode]->index_item_num = 1;
		m_hash_table[hashcode]->inverted_index = new IndexItem;
		m_hash_table[hashcode]->inverted_index->record_id = record_id;
		m_hash_table[hashcode]->inverted_index->next = NULL;
	} else {
		IndexItem *p = m_hash_table[hashcode]->inverted_index;
		IndexItem *q = NULL;
		while (p->record_id > record_id && p->next != NULL) { //降序地插入
			q = p;
			p = p->next;
		}
		if (p->record_id > record_id) {
			m_hash_table[hashcode]->index_item_num++;
			p->next = new IndexItem;
			p->next->record_id = record_id;
			p->next->next = NULL;
		} else if (p->record_id < record_id) {
			m_hash_table[hashcode]->index_item_num++;
			if (NULL == q) { //当前链表只有一个记录
				m_hash_table[hashcode]->inverted_index = new IndexItem;
				q = m_hash_table[hashcode]->inverted_index;
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

unsigned int OpenHashIndex::size_of_index() {
	unsigned int index_size = 0;
	index_size += m_table_size * sizeof(IndexHead*);
	for (unsigned int i = 0; i < m_table_size; i++) {
		if (m_hash_table[i] != NULL) {
			index_size += sizeof(*m_hash_table[i]);
			IndexItem *p = m_hash_table[i]->inverted_index;
			while (p != NULL) {
				index_size += sizeof(*p);
				p = p->next;
			}
		}
	}
	return index_size;
}

unsigned int OpenHashIndex::get_mark_record_num(const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	if (NULL == m_hash_table[hashcode]) {
		return 0;
	} else {
		return m_hash_table[hashcode]->index_item_num;
	}
}

unsigned int OpenHashIndex::get_real_record_num(const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	if (NULL == m_hash_table[hashcode]) {
		return 0;
	} else {
		unsigned int count = 0;
		IndexItem *p = m_hash_table[hashcode]->inverted_index;
		while (p != NULL) {
			count++;
			p = p->next;
		}
		return count;
	}
}

unsigned int OpenHashIndex::find_record(unsigned int *records, const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	if (NULL == m_hash_table[hashcode]) {
		return 0;
	} else {
		IndexItem *p = m_hash_table[hashcode]->inverted_index;
		unsigned int i = 0;
		for (i = 0; i < m_hash_table[hashcode]->index_item_num; i++) {
			if (NULL == p) {
				break;
			} else {
				records[i] = p->record_id;
				p = p->next;
			}
		}
		if (NULL == p && i == m_hash_table[hashcode]->index_item_num) {
			return m_hash_table[hashcode]->index_item_num;
		} else if (i < m_hash_table[hashcode]->index_item_num) { //计数值大于实际值
			return i;
		} else { //计数值小于实际值
			return get_real_record_num(key, key_length);
		}
	}
}

bool OpenHashIndex::get_key_info(char **key_info, const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	if (NULL == m_hash_table[hashcode]) {
		return false;
	} else {
		*key_info = new char[strlen(m_hash_table[hashcode]->key_info) + 1];
		strcpy(*key_info, m_hash_table[hashcode]->key_info);
		return true;
	}
}

unsigned int* OpenHashIndex::get_intersect_records(const char **keys,
		unsigned int key_num) {
	if (keys != NULL) {
		IndexItem **ptr = new IndexItem*[key_num];
		IndexItem *cur_min = NULL;
		unsigned int intersect_num = 0;
		unsigned int records_max_num = 0;
		unsigned int ptr_num = key_num;
		unsigned int hashcode;
		//准备纸带针孔，根据keys的每个值(i)来初始化跟踪指针ptr[j]，并记录针对准的孔中最小的一个cur_min和最少的记录数records_max_num，交集大小不会超过records_max_num
		for (unsigned int i = 0, j = 0; i < key_num; i++) {
			hashcode = hashfunc(keys[i], strlen(keys[i]));
			if (NULL == m_hash_table[hashcode]) {
				ptr_num--;
				continue;
			} else {
				if (j == 0) {
					cur_min = m_hash_table[hashcode]->inverted_index;
					records_max_num = m_hash_table[hashcode]->index_item_num;
				} else {
					if (m_hash_table[hashcode]->inverted_index->record_id
							< cur_min->record_id) {
						cur_min = m_hash_table[hashcode]->inverted_index;
					}
					if (m_hash_table[hashcode]->index_item_num
							< records_max_num) {
						records_max_num =
								m_hash_table[hashcode]->index_item_num;
					}
				}
				ptr[j] = m_hash_table[hashcode]->inverted_index;
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

bool OpenHashIndex::change_key_info(const char *key, size_t key_length,
		const char* key_info) {
	return false;
}

unsigned int OpenHashIndex::hashfunc(const char *str, size_t length) {
	unsigned int hashcode = m_hash_func(str, length, m_table_size);
	if (NULL == m_hash_table[hashcode])
		return hashcode;
	else {
		IndexHead *entry = m_hash_table[hashcode];
		if (entry->identifier == NULL)
			printf("id is NULL!\n");
		if (strcmp(str, m_hash_table[hashcode]->identifier) == 0) {
			return hashcode;
		} else {
			return collision_handler(str, length, hashcode);
		}
	}
}

unsigned int OpenHashIndex::collision_handler(const char *str, size_t length,
		unsigned int collision_key) {
	unsigned int hashcode;
	for (unsigned int i = 1; i < m_table_size; i++) {
//		if (log->isDebugEnabled()) {
//			log->debug("collision_key %u has %u time(s) collision",
//					collision_key, i);
//		}
		hashcode = m_probe_func(str, length, m_table_size, collision_key, i);
		if (NULL == m_hash_table[hashcode]) {
			return hashcode;
		} else {
			if (strcmp(str, m_hash_table[hashcode]->identifier) == 0) {
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

#ifdef __DEBUG__
void OpenHashIndex::print_index(const char** identifiers) {
	const char* identifier = NULL;
	OpenHashIndex& index = *this;
	IndexHead **hash_table = (IndexHead **) index.get_hash_table();
	for (unsigned int i = 0; i < index.m_table_size; i++) {
		if (hash_table[i] != NULL) {
			if (identifiers != NULL) {
				identifier = identifiers[ysk_atoi(hash_table[i]->key_info,
						strlen(hash_table[i]->key_info))];
			} else {
				identifier = hash_table[i]->identifier;
			}
			printf(
					"slot: %u\thashcode: %u\tkey: %s------Record numbers: %u------Record index: ",
					i, index.hashfunc(identifier, strlen(identifier)),
					identifier, hash_table[i]->index_item_num);
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
#endif
