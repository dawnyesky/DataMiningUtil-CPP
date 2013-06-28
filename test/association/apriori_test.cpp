/*
 * apriori_test.cpp
 *
 *  Created on: 2011-12-10
 *      Author: Yan Shankai
 */

#include <time.h>
#include <stdlib.h>
#include <libyskalgrthms/math/digit_util.h>
#include "libyskdmu/association/extractor/trade_x_extractor.h"
#include "libyskdmu/association/extractor/doc_text_extractor.h"
#include "libyskdmu/association/extractor/trade_strvv_extractor.h"
#include "libyskdmu/association/apriori.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

class HashTable: public HashIndex {
public:
	HashTable(unsigned int size) :
			HashIndex(size) {
	}
	unsigned int collision_handler(const unsigned int k_item[],
			unsigned int collision_key) const {
		return collision_key;
	}
};

void test_apriori_xml(unsigned int max_itemset_size = 3, double minsup = 0.6,
		double minconf = 0.4) {
	printf("**********Apriori with XML Data testing start testing**********\n");
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

	Apriori<Item, ItemDetail, RecordInfo> apriori;

	apriori.init(max_itemset_size, minsup, minconf);
	apriori.set_frequent_itemsets(&frequent_itemsets);
	apriori.set_assoc_rules(&assoc_rules);
	apriori.set_extractor(&trade_x);
	apriori.enable_log(print_frequent_itemsets);

	bool succeed = true;
	succeed &= apriori.apriori();
	succeed &= apriori.genrules();
	printf(succeed ? "Apriori Succeed!\n" : "Apriori Faild!\n");

	if (print_item_detial) {
		printf("ItemDetail:\n{ ");
		for (unsigned int i = 0; i < apriori.m_item_details.size(); i++) {
			printf("[%u]%s, ", i, apriori.m_item_details[i].m_identifier);
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
							(apriori.m_item_details)[iter->first[j]].m_identifier);
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
	printf("Record amount: %u\n", apriori.m_record_infos.size());
	printf(
			"Apriori with XML Data testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf("**********Apriori with XML Data finish testing**********\n\n");
}

void test_apriori_doc(unsigned int max_itemset_size = 2, double minsup = 0.05,
		double minconf = 0.4) {
	printf("**********Apriori with Doc Data testing start testing**********\n");
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

	Apriori<DocItem, DocItemDetail, DocTextRecordInfo> apriori;

	apriori.init(max_itemset_size, minsup, minconf);
	apriori.set_frequent_itemsets(&frequent_itemsets);
	apriori.set_assoc_rules(&assoc_rules);
	apriori.set_extractor(&doc_text);
	apriori.enable_log(print_frequent_itemsets);

	bool succeed = true;
	succeed &= apriori.apriori();
	succeed &= apriori.genrules();
	printf(succeed ? "Apriori Succeed!\n" : "Apriori Faild!\n");

	if (print_item_detial) {
		printf("ItemDetail:\n{ ");
		for (unsigned int i = 0; i < apriori.m_item_details.size(); i++) {
			printf("[%u]%s, ", i, apriori.m_item_details[i].m_identifier);
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
							(apriori.m_item_details)[iter->first[j]].m_identifier);
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
	printf("Record amount: %u\n", apriori.m_record_infos.size());
	printf(
			"Apriori with Doc Data testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf("**********Apriori with Doc Data finish testing**********\n\n");
}

void test_apriori_strvv(unsigned int max_itemset_size = 3, double minsup = 0.6,
		double minconf = 0.4) {
	printf(
			"**********Apriori with string table Data testing start testing**********\n");
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
	TradeStrvvExtractor trade_strvv;
	pair<vector<unsigned long long int>*, vector<vector<string> >*> data;
	unsigned long long int tids[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
	string record1[] = { "a", "b" };
	string record2[] = { "a", "c", "d", "e" };
	string record3[] = { "b", "c", "d", "f" };
	string record4[] = { "a", "b", "c", "d" };
	string record5[] = { "a", "b", "c", "f" };
	string record6[] = { "b", "c", "d", "f" };
	string record7[] = { "a", "b", "c", "d", "e" };
	string record8[] = { "a", "c", "e" };
	string record9[] = { "b", "c", "d" };
	string record10[] = { "c", "f" };
	data.first = new vector<unsigned long long int>(tids, tids + 10);
	data.second = new vector<vector<string> >();
	vector<string> record_1 = vector<string>(record1, record1 + 2);
	data.second->push_back(record_1);
	vector<string> record_2 = vector<string>(record2, record2 + 4);
	data.second->push_back(record_2);
	vector<string> record_3 = vector<string>(record3, record3 + 4);
	data.second->push_back(record_3);
	vector<string> record_4 = vector<string>(record4, record4 + 4);
	data.second->push_back(record_4);
	vector<string> record_5 = vector<string>(record5, record5 + 4);
	data.second->push_back(record_5);
	vector<string> record_6 = vector<string>(record6, record6 + 4);
	data.second->push_back(record_6);
	vector<string> record_7 = vector<string>(record7, record7 + 5);
	data.second->push_back(record_7);
	vector<string> record_8 = vector<string>(record8, record8 + 3);
	data.second->push_back(record_8);
	vector<string> record_9 = vector<string>(record9, record9 + 3);
	data.second->push_back(record_9);
	vector<string> record_10 = vector<string>(record10, record10 + 2);
	data.second->push_back(record_10);

	for (unsigned int i = 0; i < data.first->size(); i++) {
		HashTable hash_table(100);
		vector<Item> items;
		vector<string>& items_data = data.second->at(i);
		for (unsigned int j = 0; j < items_data.size(); j++) {
			unsigned int key_info = 0;
			if (hash_table.get_key_info(key_info, items_data[j].c_str(),
					items_data[j].length())) {
				items.push_back(Item(key_info));
			} else {
				ItemDetail item_detail = ItemDetail(items_data[j].c_str());
				unsigned int item_id = trade_strvv.push_detail(item_detail);
				hash_table.insert(items_data[j].c_str(), items_data[j].length(),
						item_id, item_id);
			}
		}
		RecordInfo record_info;
		record_info.tid = i;
		trade_strvv.push_record(record_info, items);
	}

	Apriori<Item, ItemDetail, RecordInfo> apriori;

	apriori.init(max_itemset_size, minsup, minconf);
	apriori.set_frequent_itemsets(&frequent_itemsets);
	apriori.set_assoc_rules(&assoc_rules);
	apriori.set_extractor(&trade_strvv);
	apriori.enable_log(print_frequent_itemsets);

	bool succeed = true;
	succeed &= apriori.apriori();
	succeed &= apriori.genrules();
	printf(succeed ? "Apriori Succeed!\n" : "Apriori Faild!\n");

	if (print_item_detial) {
		printf("ItemDetail:\n{ ");
		for (unsigned int i = 0; i < apriori.m_item_details.size(); i++) {
			printf("[%u]%s, ", i, apriori.m_item_details[i].m_identifier);
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
							(apriori.m_item_details)[iter->first[j]].m_identifier);
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

	delete data.first;
	delete data.second;

	finish = clock();
	finish_t = time(NULL);
	duration = (double) (finish - start) / (CLOCKS_PER_SEC);
	duration_t = difftime(finish_t, start_t);
	printf("Record amount: %u\n", apriori.m_record_infos.size());
	printf(
			"Apriori with string table testing duaration: %f(seconds) or %f(seconds)\n",
			duration, duration_t);
	printf("**********Apriori with string table finish testing**********\n\n");
}
