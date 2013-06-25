/*
 * cf_base.cpp
 *
 *  Created on: 2012-11-6
 *      Author: "Yan Shankai"
 */

#include <stdio.h>
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/recommend/cf_base.h"

ColFiltering::ColFiltering() {
	this->m_sim_threshold = 0;
	this->m_similarity = pearson_correlation;
}

ColFiltering::ColFiltering(
		map<unsigned int, map<unsigned int, double>*>* data_matrix) {
	this->m_data_matrix = data_matrix;
	this->m_sim_threshold = 0;
	this->m_similarity = pearson_correlation;
}

ColFiltering::~ColFiltering() {
}

vector<pair<unsigned int, double> >* ColFiltering::top_matches(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int target_obj, unsigned int k) {
	//检查参数合法性
	if (data_matrix->find(target_obj) == data_matrix->end()) {
		printf("the target object is not in the data!\n");
		return NULL;
	}
	if (k > data_matrix->size() - 1) {
		k = data_matrix->size() - 1;
	}

	unsigned int item_index[data_matrix->size() - 1];
	double scores[data_matrix->size() - 1];
	unsigned int i = 0;
	//把目标与其余对象进行对比，算出他们之间的相似度
	for (map<unsigned int, map<unsigned int, double>*>::iterator iter =
			data_matrix->begin();
			iter != data_matrix->end() && i < data_matrix->size();
			iter++, i++) {
		if (iter->first != target_obj) {
			item_index[i] = iter->first;
			scores[i] = m_similarity(data_matrix, target_obj, iter->first);
		}
	}

	//对相似度列表进行降序排序
	quicksort<double>(scores, data_matrix->size() - 1, false, false,
			item_index);

	//返回前n个对象以及他们与目标的相似度
	vector<pair<unsigned int, double> >* top_n_scores = new vector<
			pair<unsigned int, double> >();
	for (i = 0; i < k; i++) {
		top_n_scores->push_back(
				pair<unsigned int, double>(item_index[i], scores[i]));
	}

	return top_n_scores;
}

void ColFiltering::set_data_matrix(
		map<unsigned int, map<unsigned int, double>*>* data_matrix) {
	this->m_data_matrix = data_matrix;
}

void ColFiltering::set_sim_threshold(double sim_threshold) {
	this->m_sim_threshold = sim_threshold;
}

void ColFiltering::set_sim_func(SimDistance similarity) {
	this->m_similarity = similarity;
}
