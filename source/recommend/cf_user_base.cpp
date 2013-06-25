/*
 * cf_user_base.cpp
 *
 *  Created on: 2012-10-30
 *      Author: "Yan Shankai"
 */

#include <cmath>
#include <stdio.h>
#include "libyskalgrthms/math/digit_util.h"
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/recommend/cf_user_base.h"

UserBaseCF::UserBaseCF() {
}

UserBaseCF::UserBaseCF(
		map<unsigned int, map<unsigned int, double>*>* data_matrix) :
		ColFiltering(data_matrix) {

}

UserBaseCF::~UserBaseCF() {
	this->clear();
}

bool UserBaseCF::init() {
	return true;
}

void UserBaseCF::clear() {
}

vector<pair<unsigned int, double> >* UserBaseCF::get_recommendations(
		map<unsigned int, map<unsigned int, double>*>* data_matrix,
		unsigned int target_obj, unsigned int n) {
	//检查参数合法性
	if (data_matrix->find(target_obj) == data_matrix->end()) {
		printf("the target object is not in the data!\n");
		return NULL;
	}

	map<unsigned int, double> totals;
	map<unsigned int, double> sim_sums;

	//把目标与其余对象进行对比
	for (map<unsigned int, map<unsigned int, double>*>::iterator iter =
			data_matrix->begin(); iter != data_matrix->end(); iter++) {

		//不与自己比较
		if (iter->first == target_obj)
			continue;

		//算出相似度
		double sim = m_similarity(data_matrix, target_obj, iter->first);
		if (sim <= 0)
			continue;

		for (map<unsigned int, double>::iterator item_iter =
				iter->second->begin(); item_iter != iter->second->end();
				item_iter++) {

			//只对目标没有评价过的项目进行计算
			if (data_matrix->at(target_obj)->find(item_iter->first)
					!= data_matrix->at(target_obj)->end())
				continue;

			//相似度*评价值
			if (totals.find(item_iter->first) == totals.end()) {
				totals.insert(pair<unsigned int, double>(item_iter->first, 0));
			}
			totals[item_iter->first] += iter->second->at(item_iter->first)
					* sim;

			//相似度之和
			if (sim_sums.find(item_iter->first) == sim_sums.end()) {
				sim_sums.insert(
						pair<unsigned int, double>(item_iter->first, 0));
			}
			sim_sums[item_iter->first] += sim;
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

//vector<pair<unsigned int, double> >* UserBaseCF::get_recommendations(
//		map<unsigned int, map<unsigned int, double>*>* data_matrix,
//		unsigned int target_obj, unsigned int k) {
//	//检查参数合法性
//	if (data_matrix->find(target_obj) == data_matrix->end()) {
//		printf("the target object is not in the data!\n");
//		return NULL;
//	}
//
//	map<unsigned int, double> score_avgs; //每个人的评分平均值
//	map<unsigned int, double> sim_sums; //每个项目的所有人与目标相似度之和
//	map<unsigned int, double> totals; //每个项目的所有其他人评分偏差加权和
//
//	//把目标与其余对象进行对比
//	for (map<unsigned int, map<unsigned int, double>*>::iterator iter =
//			data_matrix->begin(); iter != data_matrix->end(); iter++) {
//		double score_sum = 0;
//		//初始化该用户的评分平均值
//		score_avgs.insert(pair<unsigned int, double>(iter->first, 0));
//
//		//不与自己比较
//		if (iter->first == target_obj) {
//			for (map<unsigned int, double>::iterator item_iter =
//					iter->second->begin(); item_iter != iter->second->end();
//					item_iter++) {
//				//累加该用户的评分值
//				score_sum += item_iter->second;
//			}
//			score_avgs[iter->first] = score_sum / iter->second->size();
//			continue;
//		}
//
//		//算出相似度
//		double sim = m_similarity(data_matrix->at(target_obj), iter->second);
//		double abs_sim = fabs(sim);
//		if (abs_sim <= this->m_sim_threshold)
//			continue;
//
//		for (map<unsigned int, double>::iterator item_iter =
//				iter->second->begin(); item_iter != iter->second->end();
//				item_iter++) {
//			//累加该用户的评分值
//			score_sum += item_iter->second;
//
//			//只对目标没有评价过的项目进行计算
//			if (data_matrix->at(target_obj)->find(item_iter->first)
//					!= data_matrix->at(target_obj)->end())
//				continue;
//
//			//初始化totals
//			if (totals.find(item_iter->first) == totals.end()) {
//				totals.insert(pair<unsigned int, double>(item_iter->first, 0));
//			}
//
//			//计算相似度之和
//			if (sim_sums.find(item_iter->first) == sim_sums.end()) {
//				sim_sums.insert(
//						pair<unsigned int, double>(item_iter->first, 0));
//			}
//			sim_sums[item_iter->first] += abs_sim;
//		}
//
//		//该用户的评分平均值
//		score_avgs[iter->first] = score_sum / iter->second->size();
//		//累加相似度*(评分值-平均评分值)
//		for (map<unsigned int, double>::iterator item_iter =
//				iter->second->begin();
//				item_iter != iter->second->end()
//						&& data_matrix->at(target_obj)->find(item_iter->first)
//								== data_matrix->at(target_obj)->end();
//				item_iter++) {
//			totals[item_iter->first] += (iter->second->at(item_iter->first)
//					- score_avgs[iter->first]) * sim;
//		}
//	}
//
//	//建立一个归一化的列表
//	unsigned int item_index[totals.size()];
//	double scores[totals.size()];
//	unsigned int i = 0;
//	for (map<unsigned int, double>::const_iterator iter = totals.begin();
//			iter != totals.end() && i < totals.size(); iter++, i++) {
//		item_index[i] = iter->first;
//		scores[i] = double2int(
//				score_avgs[target_obj]
//						+ totals[iter->first] / sim_sums[iter->first]);
//	}
//
//	//对列表进行降序排序
//	quicksort<double>(scores, totals.size(), false, false, item_index);
//
//	//返回前n个该对象可能会感兴趣的项目以及可能的评价
//	vector<pair<unsigned int, double> >* top_k_scores = new vector<
//			pair<unsigned int, double> >();
//	if (k == 0 || k > totals.size()) {
//		k = totals.size();
//	}
//	for (i = 0; i < k; i++) {
//		top_k_scores->push_back(
//				pair<unsigned int, double>(item_index[i], scores[i]));
//	}
//
//	return top_k_scores;
//}

map<unsigned int, map<unsigned int, double>*>* UserBaseCF::calc_recommendation(
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

		//把目标与其余对象进行对比
		for (map<unsigned int, map<unsigned int, double>*>::iterator other_iter =
				data_matrix->begin(); other_iter != data_matrix->end();
				other_iter++) {
			//不与自己比较
			if (other_iter->first == target_obj)
				continue;

			//检查相似度是否已经计算
			pair<unsigned int, unsigned int> sim_matrix_index;
			if (target_obj > other_iter->first) {
				sim_matrix_index.first = other_iter->first;
				sim_matrix_index.second = target_obj;
			} else {
				sim_matrix_index.first = target_obj;
				sim_matrix_index.second = other_iter->first;
			}

			double sim = 0;
			map<pair<unsigned int, unsigned int>, double>::const_iterator sim_iter =
					sim_matrix.find(sim_matrix_index);
			if (sim_iter == sim_matrix.end()) {
				sim = m_similarity(data_matrix, target_obj, other_iter->first);
				sim_matrix.insert(
						pair<pair<unsigned int, unsigned int>, double>(
								sim_matrix_index, sim));
			} else {
				sim = sim_iter->second;
			}

			if (sim <= this->m_sim_threshold)
				continue;

			for (map<unsigned int, double>::iterator item_iter =
					other_iter->second->begin();
					item_iter != other_iter->second->end(); item_iter++) {

				//只对目标没有评价过的项目进行计算
				if (data_matrix->at(target_obj)->find(item_iter->first)
						!= data_matrix->at(target_obj)->end())
					continue;

				//相似度*评价值
				if (totals.find(item_iter->first) == totals.end()) {
					totals.insert(
							pair<unsigned int, double>(item_iter->first, 0));
				}
				totals[item_iter->first] += other_iter->second->at(
						item_iter->first) * sim;

				//相似度之和
				if (sim_sums.find(item_iter->first) == sim_sums.end()) {
					sim_sums.insert(
							pair<unsigned int, double>(item_iter->first, 0));
				}
				sim_sums[item_iter->first] += sim;
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
		map<unsigned int, double>* prefs = NULL;
		if (rc_iter == rc_matrix->end()) {
			prefs = new map<unsigned int, double>();
			rc_matrix->insert(
					pair<unsigned int, map<unsigned int, double>*>(target_obj,
							prefs));
		} else {
			prefs = rc_iter->second;
		}

		unsigned int top_n = 0;
		if (n == 0 || n > totals.size()) {
			top_n = totals.size();
		}
		for (i = 0; i < top_n; i++) {
			prefs->insert(pair<unsigned int, double>(item_index[i], scores[i]));
		}
	}
	return rc_matrix;
}

//map<unsigned int, map<unsigned int, double>*>* UserBaseCF::calc_recommendation(
//		map<unsigned int, map<unsigned int, double>*>* data_matrix,
//		unsigned int k) {
//	map<pair<unsigned int, unsigned int>, double> sim_matrix;
//	map<unsigned int, map<unsigned int, double>*>* rc_matrix = new map<
//			unsigned int, map<unsigned int, double>*>();
//	map<unsigned int, double> score_avgs;
//	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
//			data_matrix->begin(); iter != data_matrix->end(); iter++) {
//		score_avgs.insert(pair<unsigned int, double>(iter->first, 0));
//	}
//
//	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
//			data_matrix->begin(); iter != data_matrix->end(); iter++) {
//		unsigned int target_obj = iter->first;
//
//		map<unsigned int, double> sim_sums;
//		map<unsigned int, double> totals;
//
//		//把目标与其余对象进行对比
//		for (map<unsigned int, map<unsigned int, double>*>::iterator other_iter =
//				data_matrix->begin(); other_iter != data_matrix->end();
//				other_iter++) {
//			double score_sum = 0;
//			//不与自己比较
//			if (other_iter->first == target_obj) {
//				if (score_avgs[other_iter->first] == 0) {
//					for (map<unsigned int, double>::iterator item_iter =
//							other_iter->second->begin();
//							item_iter != other_iter->second->end();
//							item_iter++) {
//						//累加该用户的评分值
//						score_sum += item_iter->second;
//					}
//					score_avgs[other_iter->first] = score_sum
//							/ other_iter->second->size();
//				}
//				continue;
//			}
//
//			//检查相似度是否已经计算
//			pair<unsigned int, unsigned int> sim_matrix_index;
//			if (target_obj > other_iter->first) {
//				sim_matrix_index.first = other_iter->first;
//				sim_matrix_index.second = target_obj;
//			} else {
//				sim_matrix_index.first = target_obj;
//				sim_matrix_index.second = other_iter->first;
//			}
//
//			double sim = 0;
//			map<pair<unsigned int, unsigned int>, double>::const_iterator sim_iter =
//					sim_matrix.find(sim_matrix_index);
//			if (sim_iter == sim_matrix.end()) {
//				sim = m_similarity(data_matrix->at(target_obj),
//						other_iter->second);
//				sim_matrix.insert(
//						pair<pair<unsigned int, unsigned int>, double>(
//								sim_matrix_index, sim));
//			} else {
//				sim = sim_iter->second;
//			}
//			double abs_sim = fabs(sim);
//
//			if (abs_sim <= this->m_sim_threshold)
//				continue;
//
//			for (map<unsigned int, double>::iterator item_iter =
//					other_iter->second->begin();
//					item_iter != other_iter->second->end(); item_iter++) {
//				//累加该用户的评分值
//				score_sum += item_iter->second;
//
//				//只对目标没有评价过的项目进行计算
//				if (data_matrix->at(target_obj)->find(item_iter->first)
//						!= data_matrix->at(target_obj)->end())
//					continue;
//
//				//初始化totals
//				if (totals.find(item_iter->first) == totals.end()) {
//					totals.insert(
//							pair<unsigned int, double>(item_iter->first, 0));
//				}
//
//				//相似度之和
//				if (sim_sums.find(item_iter->first) == sim_sums.end()) {
//					sim_sums.insert(
//							pair<unsigned int, double>(item_iter->first, 0));
//				}
//				sim_sums[item_iter->first] += abs_sim;
//			}
//
//			//该用户的评分平均值
//			if (score_avgs[other_iter->first] == 0) {
//				score_avgs[other_iter->first] = score_sum
//						/ other_iter->second->size();
//			}
//			//累加相似度*(评分值-平均评分值)
//			for (map<unsigned int, double>::iterator item_iter =
//					other_iter->second->begin();
//					item_iter != other_iter->second->end()
//							&& data_matrix->at(target_obj)->find(
//									item_iter->first)
//									== data_matrix->at(target_obj)->end();
//					item_iter++) {
//				totals[item_iter->first] += (item_iter->second
//						- score_avgs[other_iter->first]) * sim;
//			}
//		}
//
//		//建立一个归一化的列表
//		unsigned int item_index[totals.size()];
//		double scores[totals.size()];
//		unsigned int i = 0;
//		for (map<unsigned int, double>::const_iterator iter = totals.begin();
//				iter != totals.end() && i < totals.size(); iter++, i++) {
//			item_index[i] = iter->first;
//			scores[i] = double2int(
//					score_avgs[target_obj]
//							+ totals[iter->first] / sim_sums[iter->first]);
//		}
//
//		//对列表进行降序排序
//		quicksort<double>(scores, totals.size(), false, false, item_index);
//
//		//记录前n个该对象可能会感兴趣的项目以及可能的评价
//		map<unsigned int, map<unsigned int, double>*>::const_iterator rc_iter =
//				rc_matrix->find(target_obj);
//		map<unsigned int, double>* prefs = NULL;
//		if (rc_iter == rc_matrix->end()) {
//			prefs = new map<unsigned int, double>();
//			rc_matrix->insert(
//					pair<unsigned int, map<unsigned int, double>*>(target_obj,
//							prefs));
//		} else {
//			prefs = rc_iter->second;
//		}
//
//		unsigned int top_k = 0;
//		if (k == 0 || k > totals.size()) {
//			top_k = totals.size();
//		}
//		for (i = 0; i < top_k; i++) {
//			prefs->insert(pair<unsigned int, double>(item_index[i], scores[i]));
//		}
//	}
//	return rc_matrix;
//}
