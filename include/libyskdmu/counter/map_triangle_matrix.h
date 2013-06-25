/*
 * map_triangle_matrix.h
 *
 *  Created on: 2011-12-1
 *      Author: Yan Shankai
 */

#ifndef MAP_TRIANGLE_MATRIX_H_
#define MAP_TRIANGLE_MATRIX_H_

#include <map>
#include "libyskdmu/counter/counter_interface.h"

/*
 * 映射三角矩阵计数器（仅适用于2项统计）
 * 把(i, j)的计数(0<=i<j<k)存放在
 * map<unsigned int, map<unisgned int, unsigned int> > （时间优先）或
 * map<pair<unsigned int, unsigned int>, unsigned int>（空间优先）中
 *
 */
class MapTriangleMatrix: public Counter {
public:
	MapTriangleMatrix(unsigned int size);
	virtual ~MapTriangleMatrix();
	virtual bool count(const unsigned int k_item[]);
	virtual bool count(const unsigned int k_item[], unsigned int num);
	virtual unsigned int get_count(const unsigned int k_item[]) const;

private:
	std::map<unsigned int, std::map<unsigned int, unsigned int> > map_triangle_matrix;
//	std::map<std::pair<unsigned int, unsigned int>, unsigned int> map_triangle_matrix;
};

#endif /* MAP_TRIANGLE_MATRIX_H_ */
