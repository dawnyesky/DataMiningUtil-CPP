/*
 * cf_uib_test.cpp
 *
 *  Created on: 2012-11-15
 *      Author: "Yan Shankai"
 */

#include <cmath>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libyskdmu/recommend/cf_user_base.h"
#include "libyskdmu/recommend/cf_item_base.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

double test_cf_uib(double ib_coe, double ib_sim_thres, double ub_sim_thres) {
	printf(
			"**********User-Item-Based Collaborative Filtering start testing**********\n");
	start = clock();
	start_t = time(NULL);
	const char TRAIN_FILE_PATH[] = "./rs_train.txt";
	const char TEST_FILE_PATH[] = "./rs_test.txt";
	const unsigned int top_n = 0;

	unsigned int uid;
	unsigned int rid;
	double score;
	unsigned long int timestamp;
	double ub_coe = 1 - ib_coe;
	map<unsigned int, map<unsigned int, double>*> train_data_matrix;
	map<unsigned int, map<unsigned int, double>*> test_data_matrix;
	map<unsigned int, map<unsigned int, double>*> calc_data_matrix;
	map<unsigned int, map<unsigned int, double>*>* ub_calc_data_matrix;
	map<unsigned int, map<unsigned int, double>*>* ib_calc_data_matrix;
	ColFiltering* user_base_cf = new UserBaseCF();
	ColFiltering* item_base_cf = new ItemBaseCF(&train_data_matrix);

	printf("Reading train dataset...\n");
	FILE* train_fp = fopen(TRAIN_FILE_PATH, "r");
	if (NULL == train_fp) {
		printf("Failed to open train dataset file！\n");
		exit(-1);
	}
	while (!feof(train_fp)) {
		fscanf(train_fp, "%u\t%u\t%lf\t%lu\n", &uid, &rid, &score, &timestamp);
		if (train_data_matrix.find(uid) == train_data_matrix.end()) {
			train_data_matrix.insert(
					pair<unsigned int, map<unsigned int, double>*>(uid,
							new map<unsigned int, double>()));
		}
		map<unsigned int, double>* prefs = train_data_matrix.at(uid);
		prefs->insert(pair<unsigned int, double>(rid, score));
	}
	fclose(train_fp);

	user_base_cf->set_sim_threshold(ub_sim_thres);
	item_base_cf->set_sim_threshold(ib_sim_thres);

	printf("Item-Based threshold:%.4lf, User-Based threshold:%.4lf\n",
			ib_sim_thres, ub_sim_thres);
	printf("Item-Based coe:%.2lf, User-Based coe:%.2lf\n", ib_coe, ub_coe);

	printf("Calculating the similary matrix...\n");
	user_base_cf->init();
	item_base_cf->init();

	printf("Calculating the recommendation...\n");
	ib_calc_data_matrix = item_base_cf->calc_recommendation(&train_data_matrix,
			top_n);
	ub_calc_data_matrix = user_base_cf->calc_recommendation(&train_data_matrix,
			top_n);

	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			ib_calc_data_matrix->begin(); iter != ib_calc_data_matrix->end();
			iter++) {
		map<unsigned int, map<unsigned int, double>*>::iterator person_iter =
				calc_data_matrix.insert(
						pair<unsigned int, map<unsigned int, double>*>(
								iter->first, new map<unsigned int, double>())).first;
		map<unsigned int, map<unsigned int, double>*>::iterator ub_iter =
				ub_calc_data_matrix->find(iter->first);
		for (map<unsigned int, double>::const_iterator ib_iter =
				iter->second->begin(); ib_iter != iter->second->end();
				ib_iter++) {
			if (ub_iter->second->find(ib_iter->first) == ub_iter->second->end())
				continue;
			person_iter->second->insert(
					pair<unsigned int, double>(ib_iter->first,
							ub_iter->second->at(ib_iter->first) * ub_coe
									+ ib_iter->second * ib_coe));
		}
	}

//	printf("The cacl_matrix:\n");
//	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
//			cacl_data_matrix->begin(); iter != cacl_data_matrix->end();
//			iter++) {
//		printf("[%u]  ", iter->first);
//		for (map<unsigned int, double>::const_iterator prefs =
//				iter->second->begin(); prefs != iter->second->end(); prefs++) {
//			printf("<%u, %.2lf> ", prefs->first, prefs->second);
//		}
//		printf("\n");
//	}

	printf("Reading test dataset...\n");
	double sum_of_abs = 0;
	double sum_of_square = 0;
	unsigned int total_records = 0;
	FILE* test_fp = fopen(TEST_FILE_PATH, "r");
	if (NULL == test_fp) {
		printf("Failed to open test dataset file！\n");
		exit(-1);
	}
	while (!feof(test_fp)) {
		fscanf(test_fp, "%u\t%u\t%lf\t%lu\n", &uid, &rid, &score, &timestamp);
		map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
				calc_data_matrix.find(uid);
		if (iter != calc_data_matrix.end()) {
			map<unsigned int, double>::const_iterator prefs =
					iter->second->find(rid);
			if (prefs != iter->second->end()) {
				sum_of_abs += fabs(prefs->second - score);
				sum_of_square += pow(prefs->second - score, 2);
			} else {
				sum_of_abs += score;
				sum_of_square += pow(score, 2);
			}
			total_records++;
		}
//		printf("%u\t%u\t%.1lf\t%lu\n", uid, rid, score, timestamp);
	}
	fclose(test_fp);

	printf(
			"The recommendation system's Mean Average Error(MAE) value is: %.5lf\n",
			sum_of_abs / total_records);
	printf(
			"The recommendation system's Root Mean Square Error(RMSE) value is: %.5lf\n",
			sqrt(sum_of_square / total_records));

	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			train_data_matrix.begin(); iter != train_data_matrix.end();
			iter++) {
		delete iter->second;
	}
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			test_data_matrix.begin(); iter != test_data_matrix.end(); iter++) {
		delete iter->second;
	}
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			calc_data_matrix.begin(); iter != calc_data_matrix.end(); iter++) {
		delete iter->second;
	}
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			ib_calc_data_matrix->begin(); iter != ib_calc_data_matrix->end();
			iter++) {
		delete iter->second;
	}
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			ub_calc_data_matrix->begin(); iter != ub_calc_data_matrix->end();
			iter++) {
		delete iter->second;
	}

	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf(
			"User-Item-Based Collaborative Filtering testing duaration: %f(secs)\n",
			duration);
	printf(
			"**********User-Item-Based Collaborative Filtering finish testing**********\n\n");
	return sum_of_abs / total_records;
}

void find_best_coe() {
	printf("**********Finding User-Item-Based CF Coefficient**********\n");
	start = clock();
	start_t = time(NULL);

	double best_ibcoe = 0;
	double best_ibsimthres = 0;
	double best_ubsimthres = 0;
	double min_mae = 10;
	for (unsigned int ubst = 0; ubst <= 30; ubst++)
		for (unsigned int ibst = 0; ibst <= 30; ibst++)
			for (unsigned int ibcoe = 0; ibcoe <= 30; ibcoe++) {
				double ib_coe = (50 + ibcoe) / 100.0;
				double ib_st = (0 + ibst) / 100.0;
				double ub_st = (0 + ubst) / 100.0;
				double mae = test_cf_uib(ib_coe, ib_st, ub_st);
				if (mae < min_mae) {
					min_mae = mae;
					best_ibcoe = ib_coe;
					best_ibsimthres = ib_st;
					best_ubsimthres = ub_st;
				}
			}
	printf("ibcoe: %.4lf, ibst: %.4lf, ubst: %.4lf\n", best_ibcoe,
			best_ibsimthres, best_ubsimthres);
	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf("Finding User-Item-Based CF Coefficient duaration: %f(secs)\n",
			duration);
	printf(
			"**********Finish Finding User-Item-Based CF Coefficient**********\n\n");
}
