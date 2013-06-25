/*
 * similarity.h
 *
 *  Created on: 2012-9-5
 *      Author: "Yan Shankai"
 */

#ifndef SIMILARITY_H_
#define SIMILARITY_H_

#include <map>

using namespace std;

static map<unsigned int, double> col_averages;

typedef double (*SimDistance)(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int object1, unsigned int object2);

double euclidean_distance(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int object1, unsigned int object2);

double pearson_correlation(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int object1, unsigned int object2);

double pearson_correlation_uca(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int object1, unsigned int object2);

#endif /* SIMILARITY_H_ */
