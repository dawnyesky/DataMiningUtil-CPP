/*
 * doc_item.cpp
 *
 *  Created on: 2012-5-26
 *      Author: "Yan Shankai"
 */

#include <string.h>
#include "libyskdmu/association/entity/doc_item.h"

DocItem::DocItem(unsigned int index, int start_pos, int length) :
		Item(index) {
	m_pos_list.push_back(start_pos);
	m_length = length;
}

DocItem::DocItem(const DocItem& item) {
	m_index = item.m_index;
	m_count = item.m_count;
	m_pos_list = item.m_pos_list;
	m_length = item.m_length;
}

DocItem::DocItem(unsigned int index) :
		Item(index) {
}

DocItem::~DocItem() {

}

DocItem::operator unsigned int() {
	return m_index;
}

void DocItem::update(int start_pos) {
	m_pos_list.push_back(start_pos);
}

DocItemDetail::DocItemDetail(char *identifier, char *type) :
		ItemDetail(identifier) {
	m_type = new char[strlen(type) + 1];
	strcpy(m_type, type);
}

DocItemDetail::DocItemDetail(const DocItemDetail& item_detail) :
		ItemDetail(item_detail.m_identifier) {
	m_type = new char[strlen(item_detail.m_type) + 1];
	strcpy(m_type, item_detail.m_type);
}

DocItemDetail::~DocItemDetail() {
	if (m_type != NULL) {
		delete[] m_type;
	}
}

DocTextRecordInfo::DocTextRecordInfo(char* doc_name) {
	if (doc_name != NULL) {
		m_doc_name = new char[strlen(doc_name) + 1];
		strcpy(m_doc_name, doc_name);
	}
}
DocTextRecordInfo::DocTextRecordInfo(const DocTextRecordInfo& record_info) {
	if (record_info.m_doc_name != NULL) {
		m_doc_name = new char[strlen(record_info.m_doc_name) + 1];
		strcpy(m_doc_name, record_info.m_doc_name);
	}
}
DocTextRecordInfo::~DocTextRecordInfo() {
	if (m_doc_name != NULL) {
		delete[] m_doc_name;
	}
}
