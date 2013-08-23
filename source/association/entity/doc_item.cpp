/*
 * doc_item.cpp
 *
 *  Created on: 2012-5-26
 *      Author: "Yan Shankai"
 */

#include <stdlib.h>
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
	m_length = 0;
}

DocItem::~DocItem() {

}

DocItem::operator unsigned int() {
	return m_index;
}

void DocItem::update(int start_pos) {
	m_pos_list.push_back(start_pos);
}

DocItemDetail::DocItemDetail() {
	m_type = NULL;
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

int DocItemDetail::get_mpi_pack_size(MPI_Comm comm) {
	unsigned int type_length = strlen(m_type) + 1;
	int type_mpi_pack_size, result;
	MPI_Pack_size(1, MPI_UNSIGNED, comm, &result);
	MPI_Pack_size(type_length, MPI_CHAR, comm, &type_mpi_pack_size);
	result += type_mpi_pack_size;
	return ItemDetail::get_mpi_pack_size(comm) + result;
}

pair<void*, int> DocItemDetail::mpi_pack(MPI_Comm comm) {
	pair<void*, int> result;

	result.second = DocItemDetail::get_mpi_pack_size(comm);
	result.first = malloc(result.second);

	int position = 0;
	unsigned int id_len = strlen(m_identifier) + 1;
	unsigned int type_length = strlen(m_type) + 1;
	MPI_Pack(&id_len, 1, MPI_UNSIGNED, result.first, result.second, &position,
			comm);
	MPI_Pack(m_identifier, id_len, MPI_CHAR, result.first, result.second,
			&position, comm);
	MPI_Pack(&type_length, 1, MPI_UNSIGNED, result.first, result.second,
			&position, comm);
	MPI_Pack(m_type, type_length, MPI_CHAR, result.first, result.second,
			&position, comm);
	return result;
}

bool DocItemDetail::mpi_unpack(void *inbuf, int insize, int *position,
		DocItemDetail *outbuf, unsigned int outcount, MPI_Comm comm) {
	for (unsigned int i = 0; i < outcount; i++) {
		unsigned int id_len = 0;
		unsigned int type_length = 0;
		MPI_Unpack(inbuf, insize, position, &id_len, 1, MPI_UNSIGNED, comm);
		outbuf[i].m_identifier = new char[id_len];
		MPI_Unpack(inbuf, insize, position, outbuf[i].m_identifier, id_len,
				MPI_CHAR, comm);
		MPI_Unpack(inbuf, insize, position, &type_length, 1, MPI_UNSIGNED,
				comm);
		outbuf[i].m_type = new char[type_length];
		MPI_Unpack(inbuf, insize, position, outbuf[i].m_type, type_length,
				MPI_CHAR, comm);
	}
	return true;
}

DocTextRecordInfo::DocTextRecordInfo() {
	m_doc_name = NULL;
	m_tid = 0;
}

DocTextRecordInfo::DocTextRecordInfo(char* doc_name,
		unsigned long long int tid) {
	if (doc_name != NULL) {
		m_doc_name = new char[strlen(doc_name) + 1];
		strcpy(m_doc_name, doc_name);
	}
	m_tid = tid;
}
DocTextRecordInfo::DocTextRecordInfo(const DocTextRecordInfo& record_info) {
	if (record_info.m_doc_name != NULL) {
		m_doc_name = new char[strlen(record_info.m_doc_name) + 1];
		strcpy(m_doc_name, record_info.m_doc_name);
	}
	m_tid = record_info.m_tid;
}
DocTextRecordInfo::~DocTextRecordInfo() {
	if (m_doc_name != NULL) {
		delete[] m_doc_name;
	}
}

int DocTextRecordInfo::get_mpi_pack_size(MPI_Comm comm) {
	int uint_mpi_pack_size, docname_mpi_pack_size, result = 0;
	MPI_Pack_size(2, MPI_UNSIGNED, comm, &uint_mpi_pack_size); //tid和docname长度
	MPI_Pack_size(strlen(m_doc_name) + 1, MPI_CHAR, comm,
			&docname_mpi_pack_size);
	result = uint_mpi_pack_size + docname_mpi_pack_size;
	return result;
}
pair<void*, int> DocTextRecordInfo::mpi_pack(MPI_Comm comm) {
	pair<void*, int> result;

	result.second = get_mpi_pack_size(comm);
	result.first = malloc(result.second);

	int position = 0;
	MPI_Pack(&m_tid, 1, MPI_UNSIGNED, result.first, result.second, &position,
			comm);
	unsigned int docname_len = strlen(m_doc_name) + 1;
	MPI_Pack(&docname_len, 1, MPI_UNSIGNED, result.first, result.second,
			&position, comm);
	MPI_Pack(m_doc_name, docname_len, MPI_CHAR, result.first, result.second,
			&position, comm);
	return result;
}
bool DocTextRecordInfo::mpi_unpack(void *inbuf, int insize, int *position,
		DocTextRecordInfo *outbuf, unsigned int outcount, MPI_Comm comm) {
	for (unsigned int i = 0; i < outcount; i++) {
		unsigned int tid, docname_len;
		MPI_Unpack(inbuf, insize, position, &tid, 1, MPI_UNSIGNED, comm);
		outbuf[i].m_tid = tid;
		MPI_Unpack(inbuf, insize, position, &docname_len, 1, MPI_UNSIGNED,
				comm);
		outbuf[i].m_doc_name = new char[docname_len];
		MPI_Unpack(inbuf, insize, position, outbuf[i].m_doc_name, docname_len,
				MPI_CHAR, comm);
	}
	return true;
}
