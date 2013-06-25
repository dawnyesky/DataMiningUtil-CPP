/*
 * counter_interface.h
 *
 *  Created on: 2011-12-1
 *      Author: Yan Shankai
 */

#ifndef COUNTER_INTERFACE_H_
#define COUNTER_INTERFACE_H_

class Counter {
public:
	virtual ~Counter() {};
	/*
	 * description: 对k-item进行计数
	 *  parameters: item元组的索引列表
	 *      return: k-item内出现重复或者维数不符合计数器初始化时设定的维数时返回false，其余返回true
	 */
	virtual bool count(const unsigned int k_item[]) = 0;
	virtual bool count(const unsigned int k_item[], unsigned int num) = 0;
	virtual unsigned int get_count(const unsigned int k_item[]) const = 0;

protected:
	unsigned int m_size; //item元组的索引可取值的最大值
};

#endif /* COUNTER_INTERFACE_H_ */
