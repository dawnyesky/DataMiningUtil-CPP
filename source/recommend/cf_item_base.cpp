/*
 * cf_item_base.cpp
 *
 *  Created on: 2012-11-6
 *      Author: "Yan Shankai"
 */

#include <cmath>
#include <stdio.h>
#include "libyskalgrthms/math/digit_util.h"
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/recommend/cf_item_base.h"

ItemBaseCF::ItemBaseCF() {
	this->m_trans_matrix = NULL;
	this->m_sim_items = NULL;
}

ItemBaseCF::ItemBaseCF(
		map<unsigned int, map<unsigned int, double>*>* data_matrix) :
		ColFiltering(data_matrix) {
	this->m_trans_matrix = NULL;
	this->m_sim_items = NULL;
}

ItemBaseCF::~ItemBaseCF() {
	this->clear();
}

bool ItemBaseCF::init() {
	col_averages.clear();
	this->m_similarity = pearson_correlation_uca;
	this->calc_sim_items(m_data_matrix);
	return true;
}

void ItemBaseCF::clear() {
	if (this->m_trans_matrix != NULL) {
		for (map<unsigned int, map<unsigned int, double>*>::iterator iter =
				this->m_trans_matrix->begin();
				iter != this->m_trans_matrix->end(); iter++) {
			if (iter->second != NULL) {
				delete iter->second;
			}
		}
		delete m_trans_matrix;
	}

	if (this->m_sim_items != NULL) {
		for (map<unsigned int, vector<unsigned int>*>::iterator iter =
				this->m_sim_items->begin(); iter != this->m_sim_items->end();
				iter++) {
			if (iter->second != NULL) {
				delete iter->second;
			}
		}
		delete m_sim_items;
	}

	m_sim_matrix.clear();
	col_averages.clear();
}

map<unsigned int, map<unsigned int, double>*>* ItemBaseCF::transform_prefs(
		map<unsigned int, map<unsigned int, double>*>* data_matrix) {
	map<unsigned int, map<unsigned int, double>*>* trans_matrix = new map<
			unsigned int, map<unsigned int, double>*>();
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			data_matrix->begin(); iter != data_matrix->end(); iter++) {
		for (map<unsigned int, double>::const_iterator item_iter =
				iter->second->begin(); item_iter != iter->second->end();
				item_iter++) {
			map<unsigned int, map<unsigned int, double>*>::const_iterator user_rates =
					trans_matrix->find(item_iter->first);
			map<unsigned int, double>* rates = NULL;
			if (user_rates == trans_matrix->end()) {
				rates = new map<unsigned int, double>();
				trans_matrix->insert(
						pair<unsigned int, map<unsigned int, double>*>(
								item_iter->first, rates));
			} else {
				rates = user_rates->second;
			}
			rates->insert(
					pair<unsigned int, double>(iter->first, item_iter->second));
		}
	}
	return trans_matrix;
}

map<unsigned int, vector<unsigned int>*>* ItemBaseCF::calc_sim_items(
		unsigned int k) {
	if (this->m_trans_matrix == NULL) {
		printf(
				"tans_matrix is empty, please invoke the function calc_sim_matrix instead!\n");
		exit(-1);
	}

	map<unsigned int, vector<unsigned int>*>* sim_items = new map<unsigned int,
			vector<unsigned int>*>();

	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			this->m_trans_matrix->begin(); iter != this->m_trans_matrix->end();
			iter++) {
		unsigned int item_index[m_trans_matrix->size() - 1];
		double scores[m_trans_matrix->size() - 1];
		unsigned int i = 0;
		for (map<unsigned int, map<unsigned int, double>*>::const_iterator other_iter =
				this->m_trans_matrix->begin();
				other_iter != this->m_trans_matrix->end()
						&& i < m_trans_matrix->size(); other_iter++) {
			//不计算自己与自己的距离
			if (iter->first == other_iter->first) {
				continue;
			}

			//插入相似度矩阵
			pair<unsigned int, unsigned int> sim_matrix_index;
			if (iter->first > other_iter->first) {
				sim_matrix_index.first = other_iter->first;
				sim_matrix_index.second = iter->first;
			} else {
				sim_matrix_index.first = iter->first;
				sim_matrix_index.second = other_iter->first;
			}
			double sim = 0;
			map<pair<unsigned int, unsigned int>, double>::const_iterator sim_iter =
					m_sim_matrix.find(sim_matrix_index);
			if (sim_iter == m_sim_matrix.end()) {
				sim = m_similarity(m_trans_matrix, iter->first,
						other_iter->first);
				m_sim_matrix.insert(
						pair<pair<unsigned int, unsigned int>, double>(
								sim_matrix_index, sim));
			} else {
				sim = sim_iter->second;
			}

			item_index[i] = other_iter->first;
			scores[i] = sim;
			i++;
		}

		//对相似度列表进行降序排序
		quicksort<double>(scores, m_trans_matrix->size() - 1, false, false,
				item_index);

		//返回前k个对象以及他们与目标的相似度
		if (k > m_trans_matrix->size() - 1) {
			k = m_trans_matrix->size() - 1;
		}
		vector<unsigned int>* top_k_scores = new vector<unsigned int>();
		for (unsigned int j = 0; j < k; j++) {
			if (scores[j] <= this->m_sim_threshold)
				continue;
			top_k_scores->push_back(item_index[j]);
		}
		sim_items->insert(
				pair<unsigned int, vector<unsigned int>*>(iter->first,
						top_k_scores));
	}
	return sim_items;
}

void ItemBaseCF::calc_sim_items(
		map<unsigned int, map<unsigned int, double>*>* data_matrix) {
	this->clear();
	this->m_trans_matrix = this->transform_prefs(data_matrix);
	m_sim_items = calc_sim_items(m_trans_matrix->size() * (1 - this->m_sim_threshold));

//	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
//				this->m_trans_matrix->begin(); iter != this->m_trans_matrix->end();
//				iter++) {
//		for (map<unsigned int, double>::const_iterator inner_iter = iter->second->begin(); inner_iter != iter->second->end(); inner_iter++) {
//			printf("<%u,%u>[%lf]  ",iter->first, inner_iter->first, inner_iter->second);
//		}
//		printf("\n");
//	}

//	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
//			m_trans_matrix->begin(); iter != m_trans_matrix->end(); iter++) {
//		for (map<unsigned int, map<unsigned int, double>*>::const_iterator inner_iter =
//				m_trans_matrix->begin(); inner_iter != m_trans_matrix->end();
//				inner_iter++) {
//			if (iter->first < inner_iter->first) {
//				pair<unsigned int, unsigned int> sim_matrix_index;
//				sim_matrix_index.first = iter->first;
//				sim_matrix_index.second = inner_iter->first;
//				printf("<%u,%u>:%lf  ", iter->first, inner_iter->first,
//						m_sim_matrix[sim_matrix_index]);
//			}
//		}
//		printf("\n");
//	}

	return;
}

vector<pair<unsigned int, double> >* ItemBaseCF::get_recommendations(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int target_obj, unsigned int n) {
	//检查参数合法性
	if (data_matrix->find(target_obj) == data_matrix->end()) {
		printf("the target object is not in the data!\n");
		return NULL;
	}

	map<unsigned int, double> totals;
	map<unsigned int, double> sim_sums;

	map<unsigned int, double>* prefs = data_matrix->at(target_obj);
	for (map<unsigned int, double>::const_iterator iter = prefs->begin();
			iter != prefs->end(); iter++) {
		vector<unsigned int>* sim_items = m_sim_items->at(iter->first);
		for (vector<unsigned int>::const_iterator item_iter =
				sim_items->begin(); item_iter != sim_items->end();
				item_iter++) {
			//忽略已经评价过的商品
			if (prefs->find(*item_iter) != prefs->end())
				continue;

			//取出相似度
			pair<unsigned int, unsigned int> sim_matrix_index;
			if (iter->first > *item_iter) {
				sim_matrix_index.first = *item_iter;
				sim_matrix_index.second = iter->first;
			} else {
				sim_matrix_index.first = iter->first;
				sim_matrix_index.second = *item_iter;
			}
			double sim = 0;
			map<pair<unsigned int, unsigned int>, double>::const_iterator sim_iter =
					m_sim_matrix.find(sim_matrix_index);
			if (sim_iter == m_sim_matrix.end()) {
				sim = m_similarity(m_trans_matrix, iter->first, *item_iter);
				m_sim_matrix.insert(
						pair<pair<unsigned int, unsigned int>, double>(
								sim_matrix_index, sim));
			} else {
				sim = sim_iter->second;
			}

			//相似度*评价值
			if (totals.find(*item_iter) == totals.end()) {
				totals.insert(pair<unsigned int, double>(*item_iter, 0));
			}
			totals[*item_iter] += iter->second * sim;

			//相似度之和
			if (sim_sums.find(*item_iter) == sim_sums.end()) {
				sim_sums.insert(pair<unsigned int, double>(*item_iter, 0));
			}
			sim_sums[*item_iter] += sim;
		}
	}

	//建立一个归一化的列表
	unsigned int item_index[totals.size()];
	double scores[totals.size()];
	unsigned int i = 0;
	for (map<unsigned int, double>::const_iterator iter = totals.begin();
			iter != totals.end() && i < totals.size(); iter++, i++) {
		item_index[i] = iter->first;
		scores[i] = double2int(totals[iter->first] / sim_sums[iter->first]);
	}

	//对列表进行降序排序
	quicksort<double>(scores, totals.size(), false, false, item_index);

	//返回前n个该对象可能会感兴趣的项目以及可能的评价
	vector<pair<unsigned int, double> >* top_k_scores = new vector<
			pair<unsigned int, double> >();
	if (n == 0 || n > totals.size()) {
		n = totals.size();
	}
	for (i = 0; i < n; i++) {
		top_k_scores->push_back(
				pair<unsigned int, double>(item_index[i], scores[i]));
	}
	return top_k_scores;
}

map<unsigned int, map<unsigned int, double>*>* ItemBaseCF::calc_recommendation(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int n) {
	map<pair<unsigned int, unsigned int>, double> sim_matrix;
	map<unsigned int, map<unsigned int, double>*>* rc_matrix = new map<
			unsigned int, map<unsigned int, double>*>();

	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			data_matrix->begin(); iter != data_matrix->end(); iter++) {
		unsigned int target_obj = iter->first;
		map<unsigned int, double> totals;
		map<unsigned int, double> sim_sums;
		map<unsigned int, double>* prefs = data_matrix->at(target_obj);

		for (map<unsigned int, double>::const_iterator item_iter =
				prefs->begin(); item_iter != prefs->end(); item_iter++) {
			vector<unsigned int>* sim_items = m_sim_items->at(item_iter->first);
			for (vector<unsigned int>::const_iterator sim_item_iter =
					sim_items->begin(); sim_item_iter != sim_items->end();
					sim_item_iter++) {
				//忽略已经评价过的商品
				if (prefs->find(*sim_item_iter) != prefs->end())
					continue;

				//取出相似度
				pair<unsigned int, unsigned int> sim_matrix_index;
				if (item_iter->first > *sim_item_iter) {
					sim_matrix_index.first = *sim_item_iter;
					sim_matrix_index.second = item_iter->first;
				} else {
					sim_matrix_index.first = item_iter->first;
					sim_matrix_index.second = *sim_item_iter;
				}
				double sim = 0;
				map<pair<unsigned int, unsigned int>, double>::const_iterator sim_iter =
						m_sim_matrix.find(sim_matrix_index);
				if (sim_iter == m_sim_matrix.end()) {
					sim = m_similarity(m_trans_matrix, item_iter->first,
							*sim_item_iter);
					m_sim_matrix.insert(
							pair<pair<unsigned int, unsigned int>, double>(
									sim_matrix_index, sim));
				} else {
					sim = sim_iter->second;
				}

				//相似度*评价值
				if (totals.find(*sim_item_iter) == totals.end()) {
					totals.insert(
							pair<unsigned int, double>(*sim_item_iter, 0));
				}
				totals[*sim_item_iter] += item_iter->second * sim;

				//相似度之和
				if (sim_sums.find(*sim_item_iter) == sim_sums.end()) {
					sim_sums.insert(
							pair<unsigned int, double>(*sim_item_iter, 0));
				}
				sim_sums[*sim_item_iter] += sim;
			}
		}

		//建立一个归一化的列表
		unsigned int item_index[totals.size()];
		double scores[totals.size()];
		unsigned int i = 0;
		for (map<unsigned int, double>::const_iterator iter = totals.begin();
				iter != totals.end() && i < totals.size(); iter++, i++) {
			item_index[i] = iter->first;
			scores[i] = double2int(totals[iter->first] / sim_sums[iter->first]);
		}

		//对列表进行降序排序
		quicksort<double>(scores, totals.size(), false, false, item_index);

		//记录前n个该对象可能会感兴趣的项目以及可能的评价
		map<unsigned int, map<unsigned int, double>*>::const_iterator rc_iter =
				rc_matrix->find(target_obj);
		map<unsigned int, double>* rc_prefs = NULL;
		if (rc_iter == rc_matrix->end()) {
			rc_prefs = new map<unsigned int, double>();
			rc_matrix->insert(
					pair<unsigned int, map<unsigned int, double>*>(target_obj,
							rc_prefs));
		} else {
			rc_prefs = rc_iter->second;
		}

		unsigned int top_n = 0;
		if (n == 0 || n > totals.size()) {
			top_n = totals.size();
		}
		for (i = 0; i < top_n; i++) {
			rc_prefs->insert(
					pair<unsigned int, double>(item_index[i], scores[i]));
		}
	}
	return rc_matrix;
}

map<unsigned int, map<unsigned int, double>*>* ItemBaseCF::get_trans_matrix() {
	if (this->m_trans_matrix == NULL) {
		this->m_trans_matrix =
				new map<unsigned int, map<unsigned int, double>*>();
	}
	return this->m_trans_matrix;
}
