/*
 * trade_x_extractor.cpp
 *
 *  Created on: 2011-12-4
 *      Author: Yan Shankai
 */

#include <dirent.h>
#include <string.h>
#include <libxml/parser.h>
#include "libyskalgrthms/util/string.h"
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/association/extractor/trade_x_extractor.h"

TradeXmlExtractor::TradeXmlExtractor() {

}

TradeXmlExtractor::TradeXmlExtractor(vector<RecordInfo>* record_infos,
		vector<vector<Item> >* items, vector<ItemDetail>* item_details,
		HashIndex* item_index) {
	m_record_infos = record_infos;
	m_items = items;
	m_item_details = item_details;
	m_item_index = item_index;
	this->init();
}

TradeXmlExtractor::~TradeXmlExtractor() {

}

//做成守护进程，遇到新文件就调用extract_records()
void TradeXmlExtractor::read_data(bool with_hi) {
	dirent *entry = NULL;
	DIR *pDir = opendir(TRADE_INPUT_DIR);
	char fpath[strlen(TRADE_INPUT_DIR) + 256];
	m_index.clear();
	m_counter.clear();
	while (NULL != (entry = readdir(pDir))) {
		if (entry->d_type == 8
				&& strcmp(".xml", entry->d_name + (strlen(entry->d_name) - 4))
						== 0) {
			//普通文件
			strcpy(fpath, TRADE_INPUT_DIR);
			if (with_hi) {
				hi_extract_record(strcat(fpath, entry->d_name));
			} else {
				extract_record(strcat(fpath, entry->d_name));
			}
		} else {
			//目录
		}
	}
	closedir(pDir);
}

bool TradeXmlExtractor::extract_record(void* data_addr) {
	xmlDocPtr doc;
	xmlNodePtr cur_node, cur_child;
	xmlChar *tid_str = NULL;
	xmlChar *data_str = NULL;
	char* file_path = (char*) data_addr;

	doc = xmlReadFile(file_path, "utf-8", XML_PARSE_RECOVER);
	if (NULL == doc) {
		log->warn("File %s can not parsed successfully.", file_path);
		return false;
	}
	cur_node = xmlDocGetRootElement(doc);
	if (NULL == cur_node) {
		log->warn("Empty document!");
		xmlFreeDoc(doc);
		return false;
	}
	if (xmlStrcmp(cur_node->name, BAD_CAST "dataroot") != 0) {
		log->warn("Document of the wrong type, root node != dataroot");
		xmlFreeDoc(doc);
		return false;
	}
	cur_node = cur_node->children;
	while (cur_node != NULL && xmlStrcmp(cur_node->name, BAD_CAST "records")) {
		cur_node = cur_node->next;
	}
	if (NULL == cur_node) {
		log->warn("Can not find the records tag!");
		return false;
	}
	cur_node = cur_node->children;

	/* start read records loop */
	while (cur_node != NULL) {
		if (xmlStrcmp(cur_node->name, BAD_CAST "record") == 0) {
			cur_child = cur_node->children;
			//获取节点内容
			while (cur_child != NULL) {
				if (xmlStrcmp(cur_child->name, BAD_CAST "tid") == 0) {
					tid_str = xmlNodeGetContent(cur_child);
				}
				if (xmlStrcmp(cur_child->name, BAD_CAST "data") == 0) {
					data_str = xmlNodeGetContent(cur_child);
				}
				cur_child = cur_child->next;
			}

			//抽取record_info
			if (m_record_infos != NULL) {
				RecordInfo record_info;
				record_info.m_tid = atoi((char*) tid_str);
				m_record_infos->push_back(record_info);
			}

			//抽取data
			pair<char**, unsigned int> result = parse_items((char*) data_str,
					strlen((char*) data_str));
			char** items_str = result.first;
			unsigned int items_num = result.second;
			vector<Item> v_items;
			for (unsigned int i = 0; i < items_num; i++) {
				unsigned int key_info = 0;
				//抽取item_detail
				string item_str = string(items_str[i]);
				if (m_index.find(item_str) == m_index.end()) {
					m_item_details->push_back(ItemDetail(items_str[i]));
					key_info = m_item_details->size() - 1;
					m_index.insert(
							std::map<string, unsigned int>::value_type(item_str,
									key_info));
					m_counter.insert(
							std::map<string, unsigned int>::value_type(item_str,
									1));
				} else {
					key_info = m_index.at(item_str);
					m_counter.at(item_str)++;}

					//抽取item
				Item item = Item(key_info);
				pair<unsigned int, bool> bs_result = b_search<Item>(v_items,
						item);
				if (bs_result.second) {
					v_items[bs_result.first].increase();
				} else {
					v_items.insert(v_items.begin() + bs_result.first, item);
				}
			}
			if (v_items.size() > 0) {
				//回调函数
				if (m_ihandler != NULL) {
					vector<unsigned int> record;
					for (unsigned int j = 0; j < v_items.size(); j++) {
						record.push_back(v_items[j].m_index);
					}
					(this->m_ihandler)(this->m_assoc, record);
				}
				if (m_items != NULL) {
					m_items->push_back(v_items);
				}
			}

			free_char_array(result);
			xmlFree(tid_str);
			xmlFree(data_str);
		}
		cur_node = cur_node->next;
	}
	/* end read records loop */

	xmlFreeDoc(doc);
	return true;
}

bool TradeXmlExtractor::hi_extract_record(void* data_addr) {
	xmlDocPtr doc;
	xmlNodePtr cur_node, cur_child;
	xmlChar *tid_str = NULL;
	xmlChar *data_str = NULL;
	char* file_path = (char*) data_addr;

	doc = xmlReadFile(file_path, "utf-8", XML_PARSE_RECOVER);
	if (NULL == doc) {
		log->warn("File %s can not parsed successfully.", file_path);
		return false;
	}
	cur_node = xmlDocGetRootElement(doc);
	if (NULL == cur_node) {
		log->warn("Empty document!");
		xmlFreeDoc(doc);
		return false;
	}
	if (xmlStrcmp(cur_node->name, BAD_CAST "dataroot") != 0) {
		log->warn("Document of the wrong type, root node != dataroot");
		xmlFreeDoc(doc);
		return false;
	}
	cur_node = cur_node->children;
	while (cur_node != NULL && xmlStrcmp(cur_node->name, BAD_CAST "records")) {
		cur_node = cur_node->next;
	}
	if (NULL == cur_node) {
		log->warn("Can not find the records tag!");
		return false;
	}
	cur_node = cur_node->children;

	/* start read records loop */
	while (cur_node != NULL) {
		if (xmlStrcmp(cur_node->name, BAD_CAST "record") == 0) {
			cur_child = cur_node->children;
			//获取节点内容
			while (cur_child != NULL) {
				if (xmlStrcmp(cur_child->name, BAD_CAST "tid") == 0) {
					tid_str = xmlNodeGetContent(cur_child);
				}
				if (xmlStrcmp(cur_child->name, BAD_CAST "data") == 0) {
					data_str = xmlNodeGetContent(cur_child);
				}
				cur_child = cur_child->next;
			}

			//抽取record_info
			RecordInfo record_info;
			record_info.m_tid = atoi((char*) tid_str);
			m_record_infos->push_back(record_info);

			//抽取data
			pair<char**, unsigned int> result = parse_items((char*) data_str,
					strlen((char*) data_str));
			char** items_str = result.first;
			unsigned int items_num = result.second;
			vector<Item> v_items;
			for (unsigned int i = 0; i < items_num; i++) {
				char* key_info = NULL; //此处key_info指向了NULL，传入函数时要用&key_info，否则返回仍然指向NULL
				bool have_index = true;
				size_t length = strlen(items_str[i]);
				//抽取item_detail
				if (!m_item_index->get_key_info(&key_info, items_str[i],
						length)) {
					m_item_details->push_back(ItemDetail(items_str[i]));
					key_info = itoa(m_item_details->size() - 1);
					have_index = false;
				}

				//抽取item
				Item item = Item(ysk_atoi(key_info, strlen(key_info)));
				pair<unsigned int, bool> bs_result = b_search<Item>(v_items,
						item);
				if (bs_result.second) {
					v_items[bs_result.first].increase();
				} else {
					v_items.insert(v_items.begin() + bs_result.first, item);
					have_index = false;
				}

				//添加索引
				if (!have_index) {
					m_item_index->insert(items_str[i], length, key_info,
							m_record_infos->size() - 1);
				}

				if (key_info != NULL) {
					delete[] key_info;
				}
			}
			if (m_items != NULL) {
				m_items->push_back(v_items);
			}

			free_char_array(result);
			xmlFree(tid_str);
			xmlFree(data_str);
		}
		cur_node = cur_node->next;
	}
	/* end read records loop */

	xmlFreeDoc(doc);
	return true;
}

pair<char**, unsigned int> TradeXmlExtractor::parse_items(const char* data_str,
		size_t length) {
	vector<char*> items;
	unsigned int i = 0;
	unsigned int j = 0;
	unsigned int k = 0;
	while (i < length) {
		//过滤前空白格
		while (i < length && data_str[i] == ' ') {
			i++;
		}

		if (i >= length)
			break;
		j = i + 1;
		while (j < length && data_str[j] != ',') {
			j++;
		}

		k = j - 1;

		//过滤后空白格
		while (data_str[k] == ' ') {
			k--;
		}

		char *identifier = new char[k - i + 2];
		strncpy(identifier, data_str + i, k - i + 1);
		identifier[k - i + 1] = '\0';
		items.push_back(identifier);
		i = j + 1;
	}
	size_t result_size = items.size();
	char** result = new char*[result_size];
	for (unsigned int t = 0; t < result_size; t++) {
		result[t] = items[t];
	}
	return pair<char**, unsigned int>(result, result_size);
}

void TradeXmlExtractor::free_char_array(
		pair<char**, unsigned int>& char_array) {
	for (unsigned int i = 0; i < char_array.second; i++) {
		if (char_array.first[i] != NULL) {
			delete[] char_array.first[i];
		}
	}
}
