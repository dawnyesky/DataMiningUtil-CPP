/*
 * item.h
 *
 *  Created on: 2011-11-15
 *      Author: Yan Shankai
 */

#ifndef ITEM_H_
#define ITEM_H_

#include <mpi.h>
#include <vector>
#include <set>

using namespace std;

class Item {
public:
	Item();
	Item(unsigned int index);
	Item(const Item& item);
	virtual ~Item();

	operator unsigned int();

	Item& increase();
	Item& increase(unsigned int count);
	unsigned int get_count();
	virtual void update(const Item& item);

	friend bool operator<(const Item& operand1, const Item& operand2); //在容器中按index排序
	friend bool operator>(const Item& operand1, const Item& operand2);
	friend bool operator==(const Item& operand1, const Item& operand2);

public:
	unsigned int m_index; //指向ItemDetail列表的索引

protected:
	unsigned int m_count; //该Item在同一个Record里重复的次数
};

class ItemDetail { //用来存储一个Item的特有信息
public:
	ItemDetail();
	ItemDetail(const char *identifier);
	ItemDetail(const ItemDetail& item_detail);
	virtual ~ItemDetail();

	virtual int get_mpi_pack_size(MPI_Comm comm);
	virtual pair<void*, int> mpi_pack(MPI_Comm comm);
	static bool mpi_unpack(void *inbuf, int insize, int *position,
			ItemDetail *outbuf, unsigned int outcount, MPI_Comm comm);

public:
	char *m_identifier; //Item的标识符，唯一性标志，用作散列函数的关键字
};

class RecordInfo {
public:
	RecordInfo();
	RecordInfo(const unsigned long long int tid);
	RecordInfo(const RecordInfo& record_info);
	virtual ~RecordInfo();

	virtual int get_mpi_pack_size(MPI_Comm comm);
	virtual pair<void*, int> mpi_pack(MPI_Comm comm);
	static bool mpi_unpack(void *inbuf, int insize, int *position,
			RecordInfo *outbuf, unsigned int outcount, MPI_Comm comm);

public:
	unsigned long long int m_tid;
};

#endif /* ITEM_H_ */
