/*
 * comparison_test.cpp
 *
 *  Created on: 2013-6-29
 *      Author: Yan Shankai
 */

#include <time.h>
#include <stdlib.h>
#include <libyskalgrthms/math/digit_util.h>
#include <libyskalgrthms/util/string.h>
#include "libyskdmu/association/extractor/trade_x_extractor.h"
#include "libyskdmu/association/extractor/doc_text_extractor.h"
#include "libyskdmu/association/apriori.h"
#include "libyskdmu/association/fp_growth.h"
#include "libyskdmu/association/hi_apriori.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

vector<KItemsets>* apriori_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.2, double minconf = 0.4) {
	vector<KItemsets>* frequent_itemsets = new vector<KItemsets>();
	frequent_itemsets->reserve(max_itemset_size);
	vector<AssociationRule<DocItemDetail> > assoc_rules;
	DocTextExtractor doc_text;

	Apriori<DocItem, DocItemDetail, DocTextRecordInfo> apriori;

	apriori.init(max_itemset_size, minsup, minconf);
	apriori.set_frequent_itemsets(frequent_itemsets);
	apriori.set_assoc_rules(&assoc_rules);
	apriori.set_extractor(&doc_text);

	apriori.apriori();
	apriori.genrules();
	for (unsigned int i = 0; i < frequent_itemsets->size(); i++) {
		printf("Frequent %u-itemsets size of Apriori is: %u\n", i + 1,
				frequent_itemsets->at(i).get_itemsets().size());
	}
	return frequent_itemsets;
}

vector<KItemsets>* fp_growth_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.2, double minconf = 0.4) {
	vector<KItemsets>* frequent_itemsets = new vector<KItemsets>();
	frequent_itemsets->reserve(max_itemset_size);
	vector<AssociationRule<DocItemDetail> > assoc_rules;
	DocTextExtractor doc_text;

	FpGrowth<DocItem, DocItemDetail, DocTextRecordInfo> fp_growth;

	fp_growth.init(max_itemset_size, minsup, minconf);
	fp_growth.set_frequent_itemsets(frequent_itemsets);
	fp_growth.set_assoc_rules(&assoc_rules);
	fp_growth.set_extractor(&doc_text);

	fp_growth.fp_growth();
	fp_growth.genrules();
	for (unsigned int i = 0; i < frequent_itemsets->size(); i++) {
		printf("Frequent %u-itemsets size of FP-Growth is: %u\n", i + 1,
				frequent_itemsets->at(i).get_itemsets().size());
	}
	return frequent_itemsets;
}

vector<KItemsets>* hi_apriori_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.2, double minconf = 0.4) {
	vector<KItemsets>* frequent_itemsets = new vector<KItemsets>();
	frequent_itemsets->reserve(max_itemset_size);
	vector<AssociationRule<DocItemDetail> > assoc_rules;
	DocTextExtractor doc_text;

	HiAPriori<DocItem, DocItemDetail, DocTextRecordInfo> hi_apriori = HiAPriori<
			DocItem, DocItemDetail, DocTextRecordInfo>(100000);

	hi_apriori.init(max_itemset_size, minsup, minconf);
	hi_apriori.set_frequent_itemsets(frequent_itemsets);
	hi_apriori.set_assoc_rules(&assoc_rules);
	hi_apriori.set_extractor(&doc_text);
	hi_apriori.hi_apriori();
	hi_apriori.genrules();
	for (unsigned int i = 0; i < frequent_itemsets->size(); i++) {
		printf("Frequent %u-itemsets size of HI-Apriori is: %u\n", i + 1,
				frequent_itemsets->at(i).get_itemsets().size());
	}
	return frequent_itemsets;
}

void test_combine_algorithms(unsigned int max_itemset_size = 2, double minsup =
		0.2, double minconf = 0.4) {
	printf(
			"**********Combined Algorithms with Doc Data testing start testing**********\n");
	printf(
			"Max itemset's size is: %u; minimum support is: %f; minimum confidence is: %f\n",
			max_itemset_size, minsup, minconf);
	start = clock();
	start_t = time(NULL);

	const char* algorithms[3] = { "Apriori", "FP-Growth", "HI-Apriori" };
	vector<KItemsets>* apriori_fre = apriori_doc(max_itemset_size, minsup,
			minconf);
	vector<KItemsets>* fpGrowth_fre = fp_growth_doc(max_itemset_size, minsup,
			minconf);
	vector<KItemsets>* hiApriori_fre = hi_apriori_doc(max_itemset_size, minsup,
			minconf);
	for (unsigned int i = 1; i < max_itemset_size; i++) {
		printf(
				"Difference of Apriori, FP-Growth and HI-Apriori on frequent %u-itemsets:\n",
				i + 1);
		vector<map<vector<unsigned int>, unsigned int> > fre_itemsets;
		fre_itemsets.push_back(apriori_fre->at(i).get_itemsets());
		fre_itemsets.push_back(fpGrowth_fre->at(i).get_itemsets());
		fre_itemsets.push_back(hiApriori_fre->at(i).get_itemsets());
		//找频繁项集个数最小的那个算法，以此为对照基准
		unsigned int min_size = 0;
		for (unsigned int j = 1; j < fre_itemsets.size(); j++) {
			if (fre_itemsets[j].size() < fre_itemsets[0].size())
				min_size = j;
		}
		if (min_size != 0) {
			map<vector<unsigned int>, unsigned int> temp = fre_itemsets[0];
			fre_itemsets[0] = fre_itemsets[min_size];
			fre_itemsets[min_size] = temp;
			const char* temp_algorithm = algorithms[0];
			algorithms[0] = algorithms[min_size];
			algorithms[min_size] = temp_algorithm;
		}

		Apriori<DocItem, DocItemDetail, DocTextRecordInfo> temp_util;
		for (unsigned int j = 1; j < fre_itemsets.size(); j++) {
			for (map<vector<unsigned int>, unsigned int>::iterator iter1 =
					fre_itemsets[0].begin(); iter1 != fre_itemsets[0].end();
					iter1++) {
				int itemset[iter1->first.size()];
				for (unsigned int t = 0; t < iter1->first.size(); t++) {
					itemset[t] = iter1->first.data()[t];
				}

				map<vector<unsigned int>, unsigned int>::iterator iter2 =
						fre_itemsets[j].find(iter1->first);
				if (iter2 != fre_itemsets[j].end()) {
					if (iter1->second != iter2->second) {
						printf(
								"Itemset:{%s} has support %u in %s compared to %s with %u\n",
								ivtoa(itemset, iter1->first.size(), ","),
								iter2->second, algorithms[j], algorithms[0],
								iter1->second);
					}
					fre_itemsets[j].erase(iter2);
				} else {
					printf("Itemset:{%s} missing in %s compared to %s\n",
							ivtoa(itemset, iter1->first.size(), ","),
							algorithms[j], algorithms[0]);
				}
			}
			if (fre_itemsets[j].size() > 0) {
				printf("The %u redundant itemsets of %s compared to %s:\n",
						fre_itemsets[j].size(), algorithms[j], algorithms[0]);
				for (map<vector<unsigned int>, unsigned int>::iterator iter =
						fre_itemsets[j].begin(); iter != fre_itemsets[j].end();
						iter++) {
					int itemset[iter->first.size()];
					for (unsigned int t = 0; t < iter->first.size(); t++) {
						itemset[t] = iter->first.data()[t];
					}
					printf("{%s} : %u\n",
							ivtoa(itemset, iter->first.size(), ","),
							iter->second);
				}
			}
		}
	}

	delete apriori_fre;
	delete fpGrowth_fre;
	delete hiApriori_fre;
	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf(
			"Combined Algorithms with Doc Data testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf(
			"**********Combined Algorithms with Doc Data finish testing**********\n\n");
}
