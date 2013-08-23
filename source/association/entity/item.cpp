/*
 * item.cpp
 *
 *  Created on: 2011-11-15
 *      Author: Yan Shankai
 */

#include <algorithm>
#include <string.h>
#include "libyskdmu/association/entity/item.h"

using namespace std;

Item::Item() :
		m_index(0), m_count(0) {
}
Item::Item(unsigned int index) :
		m_index(index), m_count(1) {
}

Item::Item(const Item& item) {
	m_index = item.m_index;
	m_count = item.m_count;
}

Item::~Item() {

}

Item::operator unsigned int() {
	return m_index;
}

Item& Item::increase() {
	m_count++;
	return *this;
}

Item& Item::increase(unsigned int count) {
	m_count += count;
	return *this;
}

unsigned int Item::get_count() {
	return m_count;
}

void Item::update(const Item& item) {

}

bool operator<(const Item& operand1, const Item& operand2) {
	return operand1.m_index < operand2.m_index;
}

bool operator>(const Item& operand1, const Item& operand2) {
	return operand1.m_index > operand2.m_index;
}

bool operator==(const Item& operand1, const Item& operand2) {
	return operand1.m_index == operand2.m_index;
}

ItemDetail::ItemDetail() {
	m_identifier = NULL;
}

ItemDetail::ItemDetail(const char *identifier) {
	m_identifier = new char[strlen(identifier) + 1];
	strcpy(m_identifier, identifier);
}

ItemDetail::ItemDetail(const ItemDetail& item_detail) {
	m_identifier = new char[strlen(item_detail.m_identifier) + 1];
	strcpy(m_identifier, item_detail.m_identifier);
}

ItemDetail::~ItemDetail() {
	if (m_identifier != NULL) {
		delete[] m_identifier;
	}
}

int ItemDetail::get_mpi_pack_size(MPI_Comm comm) {
	int result = 0;
	MPI_Pack_size(1, MPI_UNSIGNED, comm, &result);
	int id_mpi_pack_size = 0;
	MPI_Pack_size(strlen(m_identifier) + 1, MPI_CHAR, comm, &id_mpi_pack_size);
	result += id_mpi_pack_size;
	return result;
}

pair<void*, int> ItemDetail::mpi_pack(MPI_Comm comm) {
	pair<void*, int> result;

	result.second = ItemDetail::get_mpi_pack_size(comm);
	result.first = malloc(result.second);

	int position = 0;
	unsigned int id_len = strlen(m_identifier) + 1;
	MPI_Pack(&id_len, 1, MPI_UNSIGNED, result.first, result.second, &position,
			comm);
	MPI_Pack(m_identifier, id_len, MPI_CHAR, result.first, result.second,
			&position, comm);
	return result;
}

bool ItemDetail::mpi_unpack(void *inbuf, int insize, int *position,
		ItemDetail *outbuf, unsigned int outcount, MPI_Comm comm) {
	for (unsigned int i = 0; i < outcount; i++) {
		unsigned int id_len = 0;
		MPI_Unpack(inbuf, insize, position, &id_len, 1, MPI_UNSIGNED, comm);
		outbuf[i].m_identifier = new char[id_len];
		MPI_Unpack(inbuf, insize, position, outbuf[i].m_identifier, id_len,
				MPI_CHAR, comm);
	}
	return true;
}

RecordInfo::RecordInfo() {
	m_tid = 0;
}
RecordInfo::RecordInfo(const unsigned long long int tid) {
	m_tid = tid;
}
RecordInfo::RecordInfo(const RecordInfo& record_info) {
	m_tid = record_info.m_tid;
}
RecordInfo::~RecordInfo() {

}

int RecordInfo::get_mpi_pack_size(MPI_Comm comm) {
	int tid_mpi_pack_size, result = 0;
	MPI_Pack_size(1, MPI_UNSIGNED, comm, &tid_mpi_pack_size);
	result = tid_mpi_pack_size;
	return result;
}
pair<void*, int> RecordInfo::mpi_pack(MPI_Comm comm) {
	pair<void*, int> result;

	result.second = get_mpi_pack_size(comm);
	result.first = malloc(result.second);

	int position = 0;
	MPI_Pack(&m_tid, 1, MPI_UNSIGNED, result.first, result.second, &position,
			comm);
	return result;
}
bool RecordInfo::mpi_unpack(void *inbuf, int insize, int *position,
		RecordInfo *outbuf, unsigned int outcount, MPI_Comm comm) {
	for (unsigned int i = 0; i < outcount; i++) {
		unsigned int tid = 0;
		MPI_Unpack(inbuf, insize, position, &tid, 1, MPI_UNSIGNED, comm);
		outbuf[i].m_tid = tid;
	}
	return true;
}
