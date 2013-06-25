/*
 * map_triangle_matrix.cpp
 *
 *  Created on: 2011-12-1
 *      Author: Yan Shankai
 */

#include "libyskdmu/counter/map_triangle_matrix.h"

MapTriangleMatrix::MapTriangleMatrix(unsigned int size) {
	m_size = size;
}

MapTriangleMatrix::~MapTriangleMatrix() {

}

bool MapTriangleMatrix::count(const unsigned int k_item[]) {
	if (k_item[0] == k_item[1]) {
		return false;
	}
	unsigned int m = 0;
	unsigned int n = 0;
	if (k_item[0] < k_item[1]) {
		m = k_item[0];
		n = k_item[1];
	} else if (k_item[0] > k_item[1]) {
		m = k_item[1];
		n = k_item[0];
	}
	std::map<unsigned int, std::map<unsigned int, unsigned int> >::iterator
			row_iter = map_triangle_matrix.find(m);
	if (row_iter == map_triangle_matrix.end()) {
		std::map<unsigned int, unsigned int> matrx_row;
		map_triangle_matrix.insert(std::map<unsigned int, std::map<
				unsigned int, unsigned int> >::value_type(m, matrx_row));
		row_iter = map_triangle_matrix.find(m);
	}
	std::map<unsigned int, unsigned int>& row = row_iter->second;
	std::map<unsigned int, unsigned int>::iterator col_iter = row.find(n);
	if (col_iter == row.end()) {
		row.insert(std::map<unsigned int, unsigned int>::value_type(n, 1));
	} else {
		col_iter->second++;
	}
	return true;
}

bool MapTriangleMatrix::count(const unsigned int k_item[], unsigned int num) {
	if (k_item[0] == k_item[1]) {
		return false;
	}
	int m = 0;
	int n = 0;
	if (k_item[0] < k_item[1]) {
		m = k_item[0];
		n = k_item[1];
	} else if (k_item[0] > k_item[1]) {
		m = k_item[1];
		n = k_item[0];
	}
	std::map<unsigned int, std::map<unsigned int, unsigned int> >::iterator
			row_iter = map_triangle_matrix.find(m);
	if (row_iter == map_triangle_matrix.end()) {
		std::map<unsigned int, unsigned int> matrx_row;
		map_triangle_matrix.insert(std::map<unsigned int, std::map<
				unsigned int, unsigned int> >::value_type(m, matrx_row));
		row_iter = map_triangle_matrix.find(m);
	}
	std::map<unsigned int, unsigned int>& row = row_iter->second;
	std::map<unsigned int, unsigned int>::iterator col_iter = row.find(n);
	if (col_iter == row.end()) {
		row.insert(std::map<unsigned int, unsigned int>::value_type(n, 1));
	} else {
		col_iter->second += num;
	}
	return true;
}

unsigned int MapTriangleMatrix::get_count(const unsigned int k_item[]) const {
	if (k_item[0] == k_item[1]) {
		return 0;
	}
	int m = 0;
	int n = 0;
	if (k_item[0] < k_item[1]) {
		m = k_item[0];
		n = k_item[1];
	} else if (k_item[0] > k_item[1]) {
		m = k_item[1];
		n = k_item[0];
	}
	std::map<unsigned int, std::map<unsigned int, unsigned int> >::const_iterator
			row_iter = map_triangle_matrix.find(m);
	if (row_iter == map_triangle_matrix.end()) {
		return 0;
	} else {
		std::map<unsigned int, unsigned int>::const_iterator col_iter =
				row_iter->second.find(n);
		if (col_iter == row_iter->second.end()) {
			return 0;
		} else {
			return col_iter->second;
		}
	}
}
/*使用map<std::pair<unsigned int, unsigned int>, unsigned int> 数据结构
bool MapTriangleMatrix::count(const unsigned int k_item[]) {
	if (k_item[0] == k_item[1]) {
		return false;
	}
	int m = 0;
	int n = 0;
	if (k_item[0] < k_item[1]) {
		m = k_item[0];
		n = k_item[1];
	} else if (k_item[0] > k_item[1]) {
		m = k_item[1];
		n = k_item[0];
	}
	std::map<std::pair<unsigned int, unsigned int>, unsigned int>::iterator
			iter = map_triangle_matrix.find(
					std::pair<unsigned int, unsigned int>(m, n));
	if (iter == map_triangle_matrix.end()) {
		map_triangle_matrix.insert(
				std::map<std::pair<unsigned int, unsigned int>, unsigned int>::value_type(
						std::pair<unsigned int, unsigned int>(m, n), 1));
	} else {
		iter->second++;
	}
	return true;
}

bool MapTriangleMatrix::count(const unsigned int k_item[], unsigned int num) {
	if (k_item[0] == k_item[1]) {
		return false;
	}
	int m = 0;
	int n = 0;
	if (k_item[0] < k_item[1]) {
		m = k_item[0];
		n = k_item[1];
	} else if (k_item[0] > k_item[1]) {
		m = k_item[1];
		n = k_item[0];
	}
	std::map<std::pair<unsigned int, unsigned int>, unsigned int>::iterator
			iter = map_triangle_matrix.find(
					std::pair<unsigned int, unsigned int>(m, n));
	if (iter == map_triangle_matrix.end()) {
		map_triangle_matrix.insert(
				std::map<std::pair<unsigned int, unsigned int>, unsigned int>::value_type(
						std::pair<unsigned int, unsigned int>(m, n), 1));
	} else {
		iter->second++;
	}
	return true;
}

unsigned int MapTriangleMatrix::get_count(const unsigned int k_item[]) const {
	if (k_item[0] == k_item[1]) {
		return 0;
	}
	int m = 0;
	int n = 0;
	if (k_item[0] < k_item[1]) {
		m = k_item[0];
		n = k_item[1];
	} else if (k_item[0] > k_item[1]) {
		m = k_item[1];
		n = k_item[0];
	}
	if (map_triangle_matrix.find(std::pair<int, int>(m, n))
			== map_triangle_matrix.end()) {
		return 0;
	} else {
		return map_triangle_matrix.at(std::pair<int, int>(m, n));
	}
}*/
