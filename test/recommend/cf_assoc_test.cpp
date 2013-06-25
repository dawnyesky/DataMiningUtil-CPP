/*
 * cf_assoc_test.cpp
 *
 *  Created on: 2012-11-14
 *      Author: "Yan Shankai"
 */
#include <cmath>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libyskdmu/association/extractor/trade_text_extractor.h"
#include "libyskdmu/association/apriori.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

void test_cf_assoc_wds() {
	printf(
			"**********AssocationRule-Based Collaborative Filtering start testing**********\n");
	start = clock();
	start_t = time(NULL);
	const char TEST_FILE_PATH[] = "./shared/DataRecords/rs_test.txt";
	const unsigned int max_itemset_size = 3;
	const double minsup = 0.3;
	const double minconf = 0.8;

	unsigned int uid;
	unsigned int rid;
	double score;
	unsigned long int timestamp;
	map<unsigned int, map<unsigned int, double>*> train_data_matrix;
	map<unsigned int, map<unsigned int, double>*> test_data_matrix;
	map<unsigned int, map<unsigned int, double>*> calc_data_matrix;

	vector<KItemsets> frequent_itemsets;
	frequent_itemsets.reserve(max_itemset_size);
	vector<AssociationRule<ItemDetail> > assoc_rules;
	TradeTextExtractor trade_text;
	trade_text.set_data(&train_data_matrix);
	Apriori<Item, ItemDetail, RecordInfo> apriori;
	apriori.init(max_itemset_size, minsup, minconf);
	apriori.set_frequent_itemsets(&frequent_itemsets);
	apriori.set_assoc_rules(&assoc_rules);
	apriori.set_extractor(&trade_text);
	bool succeed = true;
	succeed &= apriori.apriori();
	succeed &= apriori.genrules();
	printf(succeed ? "Apriori Succeed!\n" : "Apriori Faild!\n");

//	for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
//		const map<vector<unsigned int>, unsigned int>& k_itemsets =
//				frequent_itemsets[i].get_itemsets();
//		printf("%u-itemset:\n", i + 1);
//		for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
//				k_itemsets.begin(); iter != k_itemsets.end(); iter++) {
//			printf("{");
//			for (unsigned int j = 0; j < iter->first.size(); j++) {
//				printf("%s,",
//						(apriori.m_item_details)[iter->first[j]].m_identifier);
//			}
//			printf("}<%u>\n", iter->second);
//		}
//		printf("\n");
//	}
//
//	for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
//		printf("Frequent %u-itemsets size is: %u\n", i + 1,
//				frequent_itemsets[i].get_itemsets().size());
//	}

//	for (unsigned int i = 0; i < assoc_rules.size(); i++) {
//		string condition = "";
//		unsigned int j = 0;
//		for (; j < assoc_rules[i].condition.size() - 1; j++) {
//			condition += assoc_rules[i].condition[j].m_identifier;
//			condition += ", ";
//		}
//		condition += assoc_rules[i].condition[j].m_identifier;
//
//		string consequent = "";
//		for (j = 0; j < assoc_rules[i].consequent.size() - 1; j++) {
//			consequent += assoc_rules[i].consequent[j].m_identifier;
//			consequent += ", ";
//		}
//		consequent += assoc_rules[i].consequent[j].m_identifier;
//		printf("Association Rules[%u]: %s ===> %s ; confidence: %f\n", i,
//				condition.c_str(), consequent.c_str(),
//				assoc_rules[i].confidence);
//	}

	printf("Calculating the recommendation...\n");
	map<unsigned int, map<unsigned int, double>*> totals;
	map<unsigned int, map<unsigned int, double>*> conf_sums;
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter = train_data_matrix.begin(); iter!=train_data_matrix.end(); iter++) {
		calc_data_matrix.insert(pair<unsigned int, map<unsigned int, double>*>(iter->first, new map<unsigned int, double>()));
		totals.insert(pair<unsigned int, map<unsigned int, double>*>(iter->first, new map<unsigned int, double>()));
		conf_sums.insert(pair<unsigned int, map<unsigned int, double>*>(iter->first, new map<unsigned int, double>()));
	}
	for (vector<AssociationRule<ItemDetail> >::const_iterator iter = assoc_rules.begin(); iter!= assoc_rules.end(); iter++) {
		for (map<unsigned int, map<unsigned int, double>*>::const_iterator person_iter = train_data_matrix.begin(); person_iter!=train_data_matrix.end(); person_iter++) {
			//用户是否满足关联规则的条件
			bool conform = true;
			for (unsigned int i = 0; i < iter->condition.size() && conform; i++) {
				conform &= (person_iter->second->find(atoi(iter->condition[i].m_identifier)) != person_iter->second->end());
			}
			if (conform) {
				for (unsigned int j = 0; j < iter->consequent.size(); j++) {
					unsigned int cons_item = atoi(iter->consequent[j].m_identifier);
					map<unsigned int, double>* total_prefs = totals[person_iter->first];
					map<unsigned int, double>* conf_sums_prefs = conf_sums[person_iter->first];
					for (unsigned int i = 0; i < iter->condition.size() && conform; i++) {
						map<unsigned int, double>::iterator total_iter = total_prefs->find(cons_item);
						if (total_iter == total_prefs->end()) {
							total_prefs->insert(pair<unsigned int, double>(cons_item, person_iter->second->at(atoi(iter->condition[i].m_identifier))*iter->confidence));
						} else {
							total_iter->second += person_iter->second->at(atoi(iter->condition[i].m_identifier))*iter->confidence;
						}
						map<unsigned int, double>::iterator conf_sum_iter = conf_sums_prefs->find(cons_item);
						if (conf_sum_iter == conf_sums_prefs->end()) {
							conf_sums_prefs->insert(pair<unsigned int, double>(cons_item, iter->confidence));
						} else {
							conf_sum_iter->second+=iter->confidence;
						}
					}
				}
			}
		}
	}

	for(map<unsigned int, map<unsigned int, double>*>::const_iterator iter = totals.begin(); iter != totals.end(); iter++) {
		for (map<unsigned int, double>::const_iterator inner_iter = iter->second->begin(); inner_iter!=iter->second->end(); inner_iter++) {
			calc_data_matrix[iter->first]->insert(pair<unsigned int, double>(inner_iter->first, inner_iter->second/conf_sums[iter->first]->at(inner_iter->first)));
		}
	}

	printf("The cacl_matrix:\n");
	for (map<unsigned int, map<unsigned int, double>*>::const_iterator iter =
			calc_data_matrix.begin(); iter != calc_data_matrix.end(); iter++) {
		printf("[%u]  ", iter->first);
		for (map<unsigned int, double>::const_iterator prefs =
				iter->second->begin(); prefs != iter->second->end(); prefs++) {
			printf("<%u, %.2lf> ", prefs->first, prefs->second);
		}
		printf("\n");
	}

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
				sum_of_abs += abs(prefs->second - score);
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
	printf(
			"AssocationRule-Based Collaborative Filtering testing duaration: %f(secs)\n",
			duration);
	printf(
			"**********AssocationRule-Based Collaborative Filtering finish testing**********\n\n");
}
