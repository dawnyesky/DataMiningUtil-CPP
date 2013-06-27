/*
 * trade_text_extractor.cpp
 *
 *  Created on: 2012-11-14
 *      Author: "Yan Shankai"
 */

#include <string.h>
#include <stdio.h>
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/association/fp_growth.h"
#include "libyskdmu/association/extractor/trade_text_extractor.h"

TradeTextExtractor::TradeTextExtractor() {
	m_data = NULL;
}

TradeTextExtractor::TradeTextExtractor(vector<RecordInfo>* record_infos,
		vector<vector<Item> >* items, vector<ItemDetail>* item_details,
		HashIndex* item_index) {
	m_record_infos = record_infos;
	m_items = items;
	m_item_details = item_details;
	m_item_index = item_index;
	m_data = NULL;
	this->init();
}

TradeTextExtractor::~TradeTextExtractor() {

}

void TradeTextExtractor::read_data(bool with_hi) {
	if (with_hi) {
		hi_extract_record((void*) "./shared/DataRecords/rs_train.txt");
	} else {
		extract_record((void*) "./shared/DataRecords/rs_train.txt");
	}
}

bool TradeTextExtractor::extract_record(void* data_addr) {
	FILE* train_fp = fopen((char*) data_addr, "r");
	unsigned int uid;
	unsigned int rid;
	double score;
	unsigned long int timestamp;
	while (!feof(train_fp)) {
		fscanf(train_fp, "%u\t%u\t%lf\t%lu\n", &uid, &rid, &score, &timestamp);
		unsigned int key_info = 0;
		//抽取item_detail
		if (m_index.find(string(itoa((int) rid, 10))) == m_index.end()) {
			m_item_details->push_back(ItemDetail(itoa((int) rid, 10)));
			key_info = m_item_details->size() - 1;
			m_index.insert(
					std::map<string, unsigned int>::value_type(
							string(itoa((int) rid, 10)), key_info));
			m_counter.insert(
					std::map<string, unsigned int>::value_type(
							string(itoa((int) rid, 10)), 1));
		} else {
			key_info = m_index.at(string(itoa((int) rid, 10)));
			m_counter.at(string(itoa((int) rid, 10)))++;}

			//整合数据
		if (m_data->find(uid) == m_data->end()) {
			m_data->insert(
					pair<unsigned int, map<unsigned int, double>*>(uid,
							new map<unsigned int, double>()));
		}
		map<unsigned int, double>* prefs = m_data->at(uid);
		prefs->insert(pair<unsigned int, double>(rid, score));
	}
	fclose(train_fp);

	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			m_data->begin(); iter != m_data->end(); iter++) {
		vector<Item> v_items;
		//抽取record_info
		if (m_record_infos != NULL) {
			RecordInfo record_info;
			record_info.tid = iter->first;
			m_record_infos->push_back(record_info);
		}

		for (map<unsigned int, double>::const_iterator item_iter =
				iter->second->begin(); item_iter != iter->second->end();
				item_iter++) {
			//抽取item
			Item item = Item(
					m_index.at(string(itoa((int) item_iter->first, 10))));
			pair<unsigned int, bool> bs_result = b_search<Item>(v_items, item);
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
	}
	return true;
}

bool TradeTextExtractor::hi_extract_record(void* data_addr) {
	return true;
}

void TradeTextExtractor::set_data(
		map<unsigned int, map<unsigned int, double>*>* data) {
	this->m_data = data;
}
