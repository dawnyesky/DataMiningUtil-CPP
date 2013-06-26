/*
 * dhp_test.cpp
 *
 *  Created on: 2012-5-29
 *      Author: "Yan Shankai"
 */

#include <time.h>
#include <stdlib.h>
#include <libyskalgrthms/math/digit_util.h>
#include "libyskdmu/association/extractor/trade_x_extractor.h"
#include "libyskdmu/association/extractor/doc_text_extractor.h"
#include "libyskdmu/association/dhp.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

void test_dhp_xml(unsigned int max_itemset_size = 3, double minsup = 0.6,
		double minconf = 0.4) {
	printf("**********DHP with XML Data testing start testing**********\n");
	printf(
			"Max itemset's size is: %u; minimum support is: %f; minimum confidence is: %f\n",
			max_itemset_size, minsup, minconf);
	start = clock();
	start_t = time(NULL);
	bool print_item_detial = false;
	bool print_itemset = false;
	bool print_frequent_itemsets = false;
	bool print_assoc_rules = false;
	vector<KItemsets> frequent_itemsets;
	frequent_itemsets.reserve(max_itemset_size);
	vector<AssociationRule<ItemDetail> > assoc_rules;
	TradeXmlExtractor trade_x;

	DHP<Item, ItemDetail, RecordInfo> dhp;

	dhp.init(max_itemset_size, minsup, minconf);
	dhp.set_frequent_itemsets(&frequent_itemsets);
	dhp.set_assoc_rules(&assoc_rules);
	dhp.set_extractor(&trade_x);
	dhp.enable_log(print_frequent_itemsets);

	bool succeed = true;
	succeed &= dhp.dhp();
	succeed &= dhp.genrules();
	printf(succeed ? "DHP Succeed!\n" : "DHP Faild!\n");

	if (print_item_detial) {
		printf("ItemDetail:\n{ ");
		for (unsigned int i = 0; i < dhp.m_item_details.size(); i++) {
			printf("[%u]%s, ", i, dhp.m_item_details[i].m_identifier);
		}
		printf("}\n");
	}

	if (print_itemset) {
		for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
			const map<vector<unsigned int>, unsigned int>& k_itemsets =
					frequent_itemsets[i].get_itemsets();
			printf("%u-itemset:\n", i + 1);
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					k_itemsets.begin(); iter != k_itemsets.end(); iter++) {
				printf("{");
				for (unsigned int j = 0; j < iter->first.size(); j++) {
					printf("%s,",
							(dhp.m_item_details)[iter->first[j]].m_identifier);
				}
				printf("}\n");
			}
			printf("\n");
		}
	}

	for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
		printf("Frequent %u-itemsets size is: %u\n", i + 1,
				frequent_itemsets[i].get_itemsets().size());
	}

	if (print_assoc_rules) {
		for (unsigned int i = 0; i < assoc_rules.size(); i++) {
			string condition = "";
			unsigned int j = 0;
			for (; j < assoc_rules[i].condition.size() - 1; j++) {
				condition += assoc_rules[i].condition[j].m_identifier;
				condition += ", ";
			}
			condition += assoc_rules[i].condition[j].m_identifier;

			string consequent = "";
			for (j = 0; j < assoc_rules[i].consequent.size() - 1; j++) {
				consequent += assoc_rules[i].consequent[j].m_identifier;
				consequent += ", ";
			}
			consequent += assoc_rules[i].consequent[j].m_identifier;
			printf("Association Rules[%u]: %s ===> %s ; confidence: %f\n", i,
					condition.c_str(), consequent.c_str(),
					assoc_rules[i].confidence);
		}
	}

	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf("Record amount: %u\n", dhp.m_record_infos.size());
	printf("DHP with XML Data testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf("**********DHP with XML Data finish testing**********\n\n");
}

void test_dhp_doc(unsigned int max_itemset_size = 2, double minsup = 0.05,
		double minconf = 0.4) {
	printf("**********DHP with Doc Data testing start testing**********\n");
	printf(
			"Max itemset's size is: %u; minimum support is: %f; minimum confidence is: %f\n",
			max_itemset_size, minsup, minconf);
	start = clock();
	start_t = time(NULL);
	bool print_item_detial = false;
	bool print_itemset = false;
	bool print_frequent_itemsets = false;
	bool print_assoc_rules = false;
	vector<KItemsets> frequent_itemsets;
	frequent_itemsets.reserve(max_itemset_size);
	vector<AssociationRule<DocItemDetail> > assoc_rules;
	DocTextExtractor doc_text;

	DHP<DocItem, DocItemDetail, DocTextRecordInfo> dhp;

	dhp.init(max_itemset_size, minsup, minconf);
	dhp.set_frequent_itemsets(&frequent_itemsets);
	dhp.set_assoc_rules(&assoc_rules);
	dhp.set_extractor(&doc_text);
	dhp.enable_log(print_frequent_itemsets);

	bool succeed = true;
	succeed &= dhp.dhp();
	succeed &= dhp.genrules();
	printf(succeed ? "DHP Succeed!\n" : "DHP Faild!\n");

	if (print_item_detial) {
		printf("ItemDetail:\n{ ");
		for (unsigned int i = 0; i < dhp.m_item_details.size(); i++) {
			printf("[%u]%s, ", i, dhp.m_item_details[i].m_identifier);
		}
		printf("}\n");
	}

	if (print_itemset) {
		for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
			const map<vector<unsigned int>, unsigned int>& k_itemsets =
					frequent_itemsets[i].get_itemsets();
			printf("%u-itemset(number: %u):\n", i + 1, k_itemsets.size());
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					k_itemsets.begin(); iter != k_itemsets.end(); iter++) {
				printf("{");
				for (unsigned int j = 0; j < iter->first.size(); j++) {
					printf("%s,",
							(dhp.m_item_details)[iter->first[j]].m_identifier);
				}
				printf("}\n");
			}
			printf("\n");
		}
	}

	for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
		printf("Frequent %u-itemsets size is: %u\n", i + 1,
				frequent_itemsets[i].get_itemsets().size());
	}

	if (print_assoc_rules) {
		for (unsigned int i = 0; i < assoc_rules.size(); i++) {
			string condition = "";
			unsigned int j = 0;
			for (; j < assoc_rules[i].condition.size() - 1; j++) {
				condition += assoc_rules[i].condition[j].m_identifier;
				condition += ", ";
			}
			condition += assoc_rules[i].condition[j].m_identifier;

			string consequent = "";
			for (j = 0; j < assoc_rules[i].consequent.size() - 1; j++) {
				consequent += assoc_rules[i].consequent[j].m_identifier;
				consequent += ", ";
			}
			consequent += assoc_rules[i].consequent[j].m_identifier;
			printf("Association Rules[%u]: %s ===> %s ; confidence: %f\n", i,
					condition.c_str(), consequent.c_str(),
					assoc_rules[i].confidence);
		}
	}

	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf("Record amount: %u\n", dhp.m_record_infos.size());
	printf("DHP with Doc Data testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf("**********DHP with Doc Data finish testing**********\n\n");
}
