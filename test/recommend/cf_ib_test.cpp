/*
 * cf_ib_test.cpp
 *
 *  Created on: 2012-11-9
 *      Author: "Yan Shankai"
 */

#include <cmath>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libyskdmu/recommend/cf_item_base.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

void test_cf_ib_wds() {
	printf(
			"**********Item-Based Collaborative Filtering start testing**********\n");
	start = clock();
	start_t = time(NULL);
	const char TRAIN_FILE_PATH[] = "./shared/DataRecords/rs_train.txt";
	const char TEST_FILE_PATH[] = "./shared/DataRecords/rs_test.txt";
	const unsigned int top_n = 0;

	unsigned int uid;
	unsigned int rid;
	double score;
	unsigned long int timestamp;
	map<unsigned int, map<unsigned int, double>*> train_data_matrix;
	map<unsigned int, map<unsigned int, double>*> test_data_matrix;
	map<unsigned int, map<unsigned int, double>*> calc_data_matrix;
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

	item_base_cf->set_sim_threshold(0.8);

	printf("Calculating the similary matrix...\n");
	item_base_cf->init();

	printf("Calculating the recommendation...\n");
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			train_data_matrix.begin(); iter != train_data_matrix.end();
			iter++) {
		map<unsigned int, double>* prefs = new map<unsigned int, double>();
		vector<pair<unsigned int, double> >* recommend_items =
				item_base_cf->get_recommendations(&train_data_matrix,
						iter->first, top_n);
		for (unsigned int i = 0; i < recommend_items->size(); i++) {
			prefs->insert(
					pair<unsigned int, double>(recommend_items->at(i).first,
							recommend_items->at(i).second));
		}
		calc_data_matrix.insert(
				pair<unsigned int, map<unsigned int, double>*>(iter->first,
						prefs));
		delete recommend_items;
	}

//	printf("The cacl_matrix:\n");
//	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
//			calc_data_matrix.begin(); iter != calc_data_matrix.end(); iter++) {
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

	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf("Item-Based Collaborative Filtering testing duaration: %f(secs)\n",
			duration);
	printf(
			"**********Item-Based Collaborative Filtering finish testing**********\n\n");
}

void test_cf_ib(int argc, char *argv[]) {
	printf(
			"**********Item-Based Collaborative Filtering start testing**********\n");
	start = clock();
	start_t = time(NULL);
	const char TRAIN_FILE_PATH[] = "./rs_train.txt";
	const char TEST_FILE_PATH[] = "./rs_test.txt";
	const unsigned int top_n = 0;

	unsigned int uid;
	unsigned int rid;
	double score;
	unsigned long int timestamp;
	map<unsigned int, map<unsigned int, double>*> train_data_matrix;
	map<unsigned int, map<unsigned int, double>*> test_data_matrix;
	map<unsigned int, map<unsigned int, double>*>* calc_data_matrix;
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

	item_base_cf->set_sim_threshold(0.03);

	printf("Calculating the similary matrix...\n");
	item_base_cf->init();

	printf("Calculating the recommendation...\n");
	calc_data_matrix = item_base_cf->calc_recommendation(&train_data_matrix,
			top_n);

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
				calc_data_matrix->find(uid);
		if (iter != calc_data_matrix->end()) {
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
			calc_data_matrix->begin(); iter != calc_data_matrix->end();
			iter++) {
		delete iter->second;
	}

	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf("Item-Based Collaborative Filtering testing duaration: %f(secs)\n",
			duration);
	printf(
			"**********Item-Based Collaborative Filtering finish testing**********\n\n");
}
