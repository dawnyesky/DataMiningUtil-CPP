/*
 * array_triangle_matrix.cpp
 *
 *  Created on: 2011-12-1
 *      Author: Yan Shankai
 */

#include "libyskdmu/counter/array_triangle_matrix.h"

TriangleMatrix::TriangleMatrix(unsigned int size) {
	m_size = size;
	m_triangle_matrix = new unsigned int[m_size * (m_size + 2) - 1];
}

TriangleMatrix::~TriangleMatrix() {
	delete m_triangle_matrix;
}

bool TriangleMatrix::count(const unsigned int k_item[]) {
	unsigned int m = k_item[0] + k_item[1];
	if (k_item[0] < k_item[1]) {
		if (m % 2 != 0) { //i+j是奇数
			m_triangle_matrix[(m * m - 1) / 4 + k_item[0]]++; //(i+j)^2/4+i-1/4
		} else {
			m_triangle_matrix[m * m / 4 + k_item[0]]++; //(i+j)^2/4+i
		}
		return true;
	} else if (k_item[0] > k_item[1]) {
		if (m % 2 != 0) {
			m_triangle_matrix[(m * m - 1) / 4 + k_item[1]]++;
		} else {
			m_triangle_matrix[m * m / 4 + k_item[1]]++;
		}
		return true;
	} else {
		return false;
	}
}

bool TriangleMatrix::count(const unsigned int k_item[], unsigned int num) {
	unsigned int m = k_item[0] + k_item[1];
	if (k_item[0] < k_item[1]) {
		if (m % 2 != 0) {
			m_triangle_matrix[(m * m - 1) / 4 + k_item[0]] += num;
		} else {
			m_triangle_matrix[m * m / 4 + k_item[0]] += num;
		}
		return true;
	} else if (k_item[0] > k_item[1]) {
		if (m % 2 != 0) {
			m_triangle_matrix[(m * m - 1) / 4 + k_item[1]] += num;
		} else {
			m_triangle_matrix[m * m / 4 + k_item[1]] += num;
		}
		return true;
	} else {
		return false;
	}
}

unsigned int TriangleMatrix::get_count(const unsigned int k_item[]) const {
	int m = k_item[0] + k_item[1];
	if (k_item[0] < k_item[1]) {
		if (m % 2 != 0) {
			return m_triangle_matrix[(m * m - 1) / 4 + k_item[0]];
		} else {
			return m_triangle_matrix[m * m / 4 + k_item[0]];
		}
	} else if (k_item[0] > k_item[1]) {
		if (m % 2 != 0) {
			return m_triangle_matrix[(m * m - 1) / 4 + k_item[1]];
		} else {
			return m_triangle_matrix[m * m / 4 + k_item[1]];
		}
	} else {
		return 0;
	}

}
