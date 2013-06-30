/*
 * hi_apriori_test.cpp
 *
 *  Created on: 2012-5-31
 *      Author: "Yan Shankai"
 */

#include <time.h>
#include <stdlib.h>
#include <libyskalgrthms/math/digit_util.h>
#include "libyskdmu/association/extractor/trade_x_extractor.h"
#include "libyskdmu/association/extractor/doc_text_extractor.h"
#include "libyskdmu/association/hi_apriori.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

void test_hi_apriori_xml(unsigned int max_itemset_size = 3, double minsup = 0.6,
		double minconf = 0.4) {
	printf(
			"**********HI-Apriori with XML Data testing start testing**********\n");
	printf(
			"Max itemset's size is: %u; minimum support is: %f; minimum confidence is: %f\n",
			max_itemset_size, minsup, minconf);
	start = clock();
	start_t = time(NULL);
	bool print_item_detial = false;
	bool print_index = false;
	bool print_itemset = false;
	bool print_frequent_itemsets = false;
	bool print_assoc_rules = false;
	vector<KItemsets> frequent_itemsets;
	frequent_itemsets.reserve(max_itemset_size);
	vector<AssociationRule<ItemDetail> > assoc_rules;
	TradeXmlExtractor trade_x;

	HiAPriori<Item, ItemDetail, RecordInfo> hi_apriori = HiAPriori<Item,
			ItemDetail, RecordInfo>(1000);

	hi_apriori.init(max_itemset_size, minsup, minconf);
	hi_apriori.set_frequent_itemsets(&frequent_itemsets);
	hi_apriori.set_assoc_rules(&assoc_rules);
	hi_apriori.set_extractor(&trade_x);
	hi_apriori.enable_log(print_frequent_itemsets);

	if (print_index) {
		printf("Index:\n");
		IndexHead **hash_table = hi_apriori.m_item_index->get_hash_table();
		const char *identifier = NULL;
		for (unsigned int i = 0; i < 1000; i++) {
			if (hash_table[i] != NULL) {
				identifier =
						hi_apriori.m_item_details[hash_table[i]->key_info].m_identifier;
				printf("slot: %u\thashcode: %u\tkey: %s------Record index: ", i,
						hi_apriori.m_item_index->hashfunc(identifier,
								strlen(identifier)), identifier);
				IndexItem *p = hash_table[i]->inverted_index;
				while (p != NULL) {
					printf("%u, ", p->record_id);
					p = p->next;
				}
				printf("\n");
			}
		}
		printf("\n");
	}

	bool succeed = true;
	succeed &= hi_apriori.hi_apriori();
	succeed &= hi_apriori.genrules();
	printf(succeed ? "HI-Apriori Succeed!\n" : "HI-Apriori Faild!\n");

	if (print_item_detial) {
		printf("ItemDetail:\n{ ");
		for (unsigned int i = 0; i < hi_apriori.m_item_details.size(); i++) {
			printf("[%u]%s, ", i, hi_apriori.m_item_details[i].m_identifier);
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
							(hi_apriori.m_item_details)[iter->first[j]].m_identifier);
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
	printf("Record amount: %u\n", hi_apriori.m_record_infos.size());
	printf(
			"HI-Apriori with XML Data testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf("**********HI-Apriori with XML Data finish testing**********\n\n");
}

void test_hi_apriori_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.05, double minconf = 0.4) {
	printf(
			"**********HI-Apriori with Doc Data testing start testing**********\n");
	printf(
			"Max itemset's size is: %u; minimum support is: %f; minimum confidence is: %f\n",
			max_itemset_size, minsup, minconf);
	start = clock();
	start_t = time(NULL);
	bool print_item_detial = false;
	bool print_index = false;
	bool print_itemset = false;
	bool print_frequent_itemsets = false;
	bool print_assoc_rules = false;
	vector<KItemsets> frequent_itemsets;
	frequent_itemsets.reserve(max_itemset_size);
	vector<AssociationRule<DocItemDetail> > assoc_rules;
	DocTextExtractor doc_text;

	HiAPriori<DocItem, DocItemDetail, DocTextRecordInfo> hi_apriori = HiAPriori<
			DocItem, DocItemDetail, DocTextRecordInfo>(100000);

	hi_apriori.init(max_itemset_size, minsup, minconf);
	hi_apriori.set_frequent_itemsets(&frequent_itemsets);
	hi_apriori.set_assoc_rules(&assoc_rules);
	hi_apriori.set_extractor(&doc_text);
	hi_apriori.enable_log(print_frequent_itemsets);

	if (print_index) {
		printf("Index:\n");
		IndexHead **hash_table = hi_apriori.m_item_index->get_hash_table();
		const char *identifier = NULL;
		for (unsigned int i = 0; i < 1000; i++) {
			if (hash_table[i] != NULL) {
				identifier =
						hi_apriori.m_item_details[hash_table[i]->key_info].m_identifier;
				printf("slot: %u\thashcode: %u\tkey: %s------Record index: ", i,
						hi_apriori.m_item_index->hashfunc(identifier,
								strlen(identifier)), identifier);
				IndexItem *p = hash_table[i]->inverted_index;
				while (p != NULL) {
					printf("%u, ", p->record_id);
					p = p->next;
				}
				printf("\n");
			}
		}
		printf("\n");
	}

	bool succeed = true;
	succeed &= hi_apriori.hi_apriori();
	succeed &= hi_apriori.genrules();
	printf(succeed ? "HI-Apriori Succeed!\n" : "HI-Apriori Faild!\n");

	if (print_item_detial) {
		printf("ItemDetail:\n{ ");
		for (unsigned int i = 0; i < hi_apriori.m_item_details.size(); i++) {
			printf("[%u]%s, ", i, hi_apriori.m_item_details[i].m_identifier);
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
							(hi_apriori.m_item_details)[iter->first[j]].m_identifier);
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
	printf("Record amount: %u\n", hi_apriori.m_record_infos.size());
	printf(
			"HI-Apriori with Doc Data testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf("**********HI-Apriori with Doc Data finish testing**********\n\n");
}
