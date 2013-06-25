/*
 * cf_ub_test.cpp
 *
 *  Created on: 2012-10-30
 *      Author: "Yan Shankai"
 */

#include <cmath>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libyskdmu/recommend/cf_user_base.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

void test_cf_ub() {
	printf(
			"**********User-Based Collaborative Filtering start testing**********\n");
	start = clock();
	start_t = time(NULL);
	const unsigned int person_num = 5;
	const unsigned int item_num = 5;
	const unsigned int record_num = 15;
	const unsigned int target_person = 0;
	const unsigned int top_n = 3;
	const char* person[person_num] = { "Caspar", "Sally", "Camily", "Lucy",
			"Amy" };
	const char* item[item_num] = { "chicken", "dog", "bumblebee", "saber",
			"archer" };
	const unsigned int person_index[record_num] = { 1, 3, 4, 3, 2, 1, 3, 2, 0,
			4, 3, 0, 2, 1, 4 };
	const unsigned int item_index[record_num] = { 0, 2, 1, 4, 0, 3, 3, 2, 1, 3,
			0, 3, 4, 2, 4 };
	const double scores[record_num] = { 3.0, 4.0, 3.5, 3.0, 4.0, 2.0, 3.0, 1.5,
			3.0, 4.5, 2.5, 4.0, 1.5, 3.0, 2.5 };

	map<unsigned int, map<unsigned int, double>*> data_matrix;
	for (unsigned int i = 0; i < record_num; i++) {
		if (data_matrix.find(person_index[i]) == data_matrix.end()) {
			data_matrix.insert(
					pair<unsigned int, map<unsigned int, double>*>(
							person_index[i], new map<unsigned int, double>()));
		}
		map<unsigned int, double>* prefs = data_matrix.at(person_index[i]);
		prefs->insert(pair<unsigned int, double>(item_index[i], scores[i]));
	}

	ColFiltering* user_base_cf = new UserBaseCF();
	user_base_cf->init();
	vector<pair<unsigned int, double> >* recommend_items =
			user_base_cf->get_recommendations(&data_matrix, target_person,
					top_n);

	printf("The recommendations for %s are:\n", person[target_person]);
	for (unsigned int i = 0; i < recommend_items->size(); i++) {
		printf("%.4lf, %s\n", recommend_items->at(i).second,
				item[recommend_items->at(i).first]);
	}

	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			data_matrix.begin(); iter != data_matrix.end(); iter++) {
		delete iter->second;
	}
	delete recommend_items;

	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf("User-Based Collaborative Filtering testing duaration: %f(secs)\n",
			duration);
	printf(
			"**********User-Based Collaborative Filtering finish testing**********\n\n");
}

void test_cf_ub_wds() {
	printf(
			"**********User-Based Collaborative Filtering start testing**********\n");
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
	ColFiltering* user_base_cf = new UserBaseCF();

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
//		printf("%u\t%u\t%.1lf\t%lu\n", uid, rid, score, timestamp);
	}
	fclose(train_fp);

	user_base_cf->set_sim_threshold(0.2);

	printf("Calculating the recommendation...\n");
	user_base_cf->init();
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			train_data_matrix.begin(); iter != train_data_matrix.end();
			iter++) {
		map<unsigned int, double>* prefs = new map<unsigned int, double>();
		vector<pair<unsigned int, double> >* recommend_items =
				user_base_cf->get_recommendations(&train_data_matrix,
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
//			cacl_data_matrix.begin(); iter != cacl_data_matrix.end(); iter++) {
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
	printf("User-Based Collaborative Filtering testing duaration: %f(secs)\n",
			duration);
	printf(
			"**********User-Based Collaborative Filtering finish testing**********\n\n");
}

void test_cf_ub(int argc, char *argv[]) {
	printf(
			"**********User-Based Collaborative Filtering start testing**********\n");
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
	ColFiltering* user_base_cf = new UserBaseCF();

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
//		printf("%u\t%u\t%.1lf\t%lu\n", uid, rid, score, timestamp);
	}
	fclose(train_fp);

	user_base_cf->set_sim_threshold(0);

	printf("Calculating the recommendation...\n");
	user_base_cf->init();
	calc_data_matrix = user_base_cf->calc_recommendation(&train_data_matrix,
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
	printf("User-Based Collaborative Filtering testing duaration: %f(secs)\n",
			duration);
	printf(
			"**********User-Based Collaborative Filtering finish testing**********\n\n");
}
