/*
 * ro_dhi_data.h
 *
 *  Created on: 2013-10-11
 *      Author: Yan Shankai
 */

#ifndef RO_DHI_DATA_H_
#define RO_DHI_DATA_H_

#include "libyskdmu/index/ro_hi_data.h"

class RODynamicHashIndexData: public ROHashIndexData {
public:
	RODynamicHashIndexData();
	virtual ~RODynamicHashIndexData();
	virtual bool build(HashIndex* original_index);
	/*
	 * description: 对象成员数据填充函数，用以一次获取类内所有成员数据
	 *  parameters: deep:			全局深度
	 *  			data:			索引数据区指针
	 *  			data_size:		索引数据区大小
	 *				l1_index:		一级索引指针
	 *				l1_index_size:	一级索引大小
	 *				l2_index：		二级索引指针
	 *				l2_index_size：	二级索引大小
	 *      return: 填充是否成功
	 */
	virtual bool fill_memeber_data(unsigned int** deep, unsigned int** data,
			unsigned int** data_size, unsigned int** l1_index,
			unsigned int** l1_index_size, unsigned char** l2_index,
			unsigned int** l2_index_size);

protected:
	unsigned int m_d; //全局位深度D
	unsigned int* m_l1_index; //索引数据块的一级索引
	unsigned int m_l1_index_size; //一级索引大小
	void* m_l2_index; //索引数据块的二级索引
	unsigned int m_l2_index_size;
};

#endif /* RO_DHI_DATA_H_ */
