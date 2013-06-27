/*
 * trade_strvv_extractor.cpp
 *
 *  Created on: 2012-8-14
 *      Author: "Yan Shankai"
 */

#include <string.h>
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/association/fp_growth.h"
#include "libyskdmu/association/extractor/trade_strvv_extractor.h"

TradeStrvvExtractor::TradeStrvvExtractor() {

}

TradeStrvvExtractor::TradeStrvvExtractor(vector<RecordInfo>* record_infos,
		vector<vector<Item> >* items, vector<ItemDetail>* item_details,
		HashIndex* item_index) {
	m_record_infos = record_infos;
	m_items = items;
	m_item_details = item_details;
	m_item_index = item_index;
	this->init();
}

TradeStrvvExtractor::~TradeStrvvExtractor() {

}

void TradeStrvvExtractor::read_data(bool with_hi) {
	if (m_data.first.size() == 0 || m_data.second.size() == 0
			|| m_data_details.size() == 0) {
		return;
	}
	if (m_data.first.size() != m_data.second.size()) {
		return;
	}
	m_index.clear();
	m_counter.clear();

	for (unsigned int i = 0; i < m_data.first.size(); i++) {
		pair<RecordInfo*, vector<Item>*>* record = new pair<RecordInfo*,
				vector<Item>*>();
		record->first = &(m_data.first[i]);
		record->second = &(m_data.second[i]);
		if (with_hi) {
			hi_extract_record(record);
		} else {
			extract_record(record);
		}
		delete record;
	}
}

bool TradeStrvvExtractor::extract_record(void* data_addr) {
	pair<RecordInfo*, vector<Item>*>* record =
			(pair<RecordInfo*, vector<Item>*>*) data_addr;
	//抽取record_info
	if (m_record_infos != NULL) {
		m_record_infos->push_back(*record->first);
	}

	vector<Item>* items = record->second;
	vector<Item> v_items;
	for (unsigned int i = 0; i < items->size(); i++) {
		unsigned int key_info = 0;
		//抽取item_detail
		if (m_index.find(m_data_details[items->at(i).m_index].m_identifier)
				== m_index.end()) {
			m_item_details->push_back(m_data_details[items->at(i).m_index]);
			key_info = m_item_details->size() - 1;
			m_index.insert(
					std::map<string, unsigned int>::value_type(
							m_data_details[items->at(i).m_index].m_identifier,
							key_info));
			m_counter.insert(
					std::map<string, unsigned int>::value_type(
							m_data_details[items->at(i).m_index].m_identifier,
							1));
		} else {
			key_info = m_index.at(
					m_data_details[items->at(i).m_index].m_identifier);
			m_counter.at(m_data_details[items->at(i).m_index].m_identifier)++;}

			//抽取item
		Item item = Item(key_info);
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
	return true;
}

bool TradeStrvvExtractor::hi_extract_record(void* data_addr) {
	pair<RecordInfo*, vector<Item>*>* record =
			(pair<RecordInfo*, vector<Item>*>*) data_addr;
	//抽取record_info
	if (m_record_infos != NULL) {
		m_record_infos->push_back(*record->first);
	}

	vector<Item>* items = record->second;
	vector<Item> v_items;
	for (unsigned int i = 0; i < items->size(); i++) {
		unsigned int key_info = 0;
		bool have_index = true;
		size_t length = strlen(
				m_data_details[items->at(i).m_index].m_identifier);
		//抽取item_detail
		if (!m_item_index->get_key_info(key_info,
				m_data_details[items->at(i).m_index].m_identifier, length)) {
			m_item_details->push_back(
					ItemDetail(
							m_data_details[items->at(i).m_index].m_identifier));
			key_info = m_item_details->size() - 1;
			have_index = false;
		}

		//抽取item
		Item item = Item(key_info);
		pair<unsigned int, bool> bs_result = b_search<Item>(v_items, item);
		if (bs_result.second) {
			v_items[bs_result.first].increase();
		} else {
			v_items.insert(v_items.begin() + bs_result.first, item);
			have_index = false;
		}

		//添加索引
		if (!have_index) {
			m_item_index->insert(
					m_data_details[items->at(i).m_index].m_identifier, length,
					key_info, m_record_infos->size() - 1);
		}
	}
	if (m_items != NULL) {
		m_items->push_back(v_items);
	}
	return true;
}

unsigned int TradeStrvvExtractor::push_record(RecordInfo& record,
		vector<Item>& items) {
	this->m_data.first.push_back(record);
	this->m_data.second.push_back(items);
	return m_data.first.size() - 1;
}
unsigned int TradeStrvvExtractor::push_detail(ItemDetail& detail) {
	this->m_data_details.push_back(detail);
	return m_data_details.size() - 1;
}

void TradeStrvvExtractor::clear_data_source() {
	this->m_data.first.clear();
	this->m_data.second.clear();
	this->m_data_details.clear();
}
