/*
 * doc_item.h
 *
 *  Created on: 2012-5-26
 *      Author: "Yan Shankai"
 */

#ifndef DOC_ITEM_H_
#define DOC_ITEM_H_

#include <vector>
#include "libyskdmu/association/entity/item.h"

class DocItem: public Item {
public:
	DocItem(unsigned int index, int start_pos, int length);
	DocItem(const DocItem& item);
	DocItem(unsigned int index);
	virtual ~DocItem();

	operator unsigned int();

	virtual void update(int start_pos);

public:
	vector<int> m_pos_list; //位置链表
	int m_length; //长度
};

class DocItemDetail: public ItemDetail {
public:
	DocItemDetail(char *identifier, char *type);
	DocItemDetail(const DocItemDetail& item_detail);
	virtual ~DocItemDetail();

public:
	char *m_type; //词性
};

class DocTextRecordInfo {
public:
	DocTextRecordInfo(char* doc_name);
	DocTextRecordInfo(const DocTextRecordInfo& record_info);
	virtual ~DocTextRecordInfo();

public:
	char *m_doc_name;
};

#endif /* DOC_ITEM_H_ */
