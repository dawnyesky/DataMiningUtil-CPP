/*
 * similarity.cpp
 *
 *  Created on: 2012-9-5
 *      Author: "Yan Shankai"
 */

#include <cmath>
#include "libyskdmu/cluster/similarity.h"

double euclidean_distance(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int object1, unsigned int object2) {
	unsigned int total_share = 0;
	double sum_of_squares = 0;
	map<unsigned int, double>* larger;
	map<unsigned int, double>* smaller;
	if (data_matrix->at(object1)->size() > data_matrix->at(object2)->size()) {
		larger = data_matrix->at(object1);
		smaller = data_matrix->at(object2);
	} else {
		larger = data_matrix->at(object2);
		smaller = data_matrix->at(object1);
	}
	for (map<unsigned int, double>::const_iterator iter = smaller->begin();
			iter != smaller->end(); iter++) {
		map<unsigned int, double>::const_iterator shared_attribute =
				larger->find(iter->first);
		if (shared_attribute != larger->end()) {
			//计算所有差值的平方和
			sum_of_squares += pow(iter->second - shared_attribute->second, 2);
			total_share++;
		}
	}
	//两者没有共同之处
	if (total_share == 0) {
		return 0;
	} else {
		return 1 / (1 + sqrt(sum_of_squares));
	}
}

double pearson_correlation(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int object1, unsigned int object2) {
	unsigned int total_share = 0;
	double sum1 = 0, sum2 = 0;
	double sum_of_squares1 = 0, sum_of_squares2 = 0; //平方和
	double sum_of_product = 0; //两个对象共同项目评分的乘积之和
	map<unsigned int, double>* larger;
	map<unsigned int, double>* smaller;
	if (data_matrix->at(object1)->size() > data_matrix->at(object2)->size()) {
		larger = data_matrix->at(object1);
		smaller = data_matrix->at(object2);
	} else {
		larger = data_matrix->at(object2);
		smaller = data_matrix->at(object1);
	}
	for (map<unsigned int, double>::const_iterator iter = smaller->begin();
			iter != smaller->end(); iter++) {
		map<unsigned int, double>::const_iterator shared_attribute =
				larger->find(iter->first);
		if (shared_attribute != larger->end()) {
			sum1 += iter->second;
			sum2 += shared_attribute->second;
			sum_of_squares1 += pow(iter->second, 2);
			sum_of_squares2 += pow(shared_attribute->second, 2);
			sum_of_product += iter->second * shared_attribute->second;
			total_share++;
		}
	}
	//两者没有共同之处
	if (total_share == 0) {
		return 0;
	} else {
		double divisor = sqrt(
				(sum_of_squares1 - pow(sum1, 2) / total_share)
						* (sum_of_squares2 - pow(sum2, 2) / total_share));
		if (divisor == 0)
			return 1;
		else
			return (sum_of_product - sum1 * sum2 / total_share) / divisor;
	}
}

double pearson_correlation_uca(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int object1, unsigned int object2) {
	if (col_averages.size() == 0) {
		map<unsigned int, double> sums;
		map<unsigned int, double> totals;
		for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
				data_matrix->begin(); iter != data_matrix->end(); iter++) {
			for (map<unsigned int, double>::const_iterator inner_iter =
					iter->second->begin(); inner_iter != iter->second->end();
					inner_iter++) {
				map<unsigned int, double>::iterator sum = sums.find(
						inner_iter->first);
				if (sum == sums.end()) {
					sums.insert(
							pair<unsigned int, double>(inner_iter->first,
									inner_iter->second));
				} else {
					sum->second += inner_iter->second;
				}
				map<unsigned int, double>::iterator total = totals.find(
						inner_iter->first);
				if (total == totals.end()) {
					totals.insert(
							pair<unsigned int, double>(inner_iter->first, 1));
				} else {
					total->second++;
				}
			}
		}
		for (map<unsigned int, double>::const_iterator iter = sums.begin();
				iter != sums.end(); iter++) {
			col_averages.insert(
					pair<unsigned int, double>(iter->first,
							iter->second / totals[iter->first]));
		}
	}
	unsigned int total_share = 0;
	double sum = 0;
	double sum_of_squares1 = 0, sum_of_squares2 = 0; //平方和
	map<unsigned int, double>* larger;
	map<unsigned int, double>* smaller;
	if (data_matrix->at(object1)->size() > data_matrix->at(object2)->size()) {
		larger = data_matrix->at(object1);
		smaller = data_matrix->at(object2);
	} else {
		larger = data_matrix->at(object2);
		smaller = data_matrix->at(object1);
	}
	for (map<unsigned int, double>::const_iterator iter = smaller->begin();
			iter != smaller->end(); iter++) {
		map<unsigned int, double>::const_iterator shared_attribute =
				larger->find(iter->first);
		if (shared_attribute != larger->end()) {
			sum += (iter->second - col_averages[iter->first])
					* (shared_attribute->second
							- col_averages[shared_attribute->first]);
			sum_of_squares1 += pow(iter->second - col_averages[iter->first], 2);
			sum_of_squares2 += pow(
					shared_attribute->second
							- col_averages[shared_attribute->first], 2);
			total_share++;
		}
	}
	//两者没有共同之处
	if (total_share == 0) {
		return 0;
	} else {
		double divisor = sqrt(sum_of_squares1 * sum_of_squares2);
		if (divisor == 0)
			return 1;
		else
			return sum / divisor;
	}
}
