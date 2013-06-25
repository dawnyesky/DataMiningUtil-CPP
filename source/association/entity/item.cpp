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

void Item::update(const Item& item) {

}

unsigned int Item::get_count() {
	return m_count;
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
		delete m_identifier;
	}
}
