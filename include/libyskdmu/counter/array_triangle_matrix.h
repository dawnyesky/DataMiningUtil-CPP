/*
 * array_triangle_matrix.h
 *
 *  Created on: 2011-12-1
 *      Author: Yan Shankai
 */

#ifndef ARRAY_TRIANGLE_MATRIX_H_
#define ARRAY_TRIANGLE_MATRIX_H_

#include "libyskdmu/counter/counter_interface.h"

/*
 * 三角矩阵计数器（仅适用于2项统计）
 * 把(i, j)的计数(0<=i<j<k)存放在a[n]中，其中:
 * n=(i+j)^2/4 + i - 1/4 	当i+j是奇数时
 * n=(i+j)^2/4 + i			当i+j是偶数时
 *
 * 当项目对出现的可能性大于或等于1/3时适用
 */
class TriangleMatrix: public Counter {
public:
	TriangleMatrix(unsigned int size);
	virtual ~TriangleMatrix();
	virtual bool count(const unsigned int k_item[]);
	virtual bool count(const unsigned int k_item[], unsigned int num);
	virtual unsigned int get_count(const unsigned int k_item[]) const;

private:
	unsigned int *m_triangle_matrix;
};

#endif /* ARRAY_TRIANGLE_MATRIX_H_ */
