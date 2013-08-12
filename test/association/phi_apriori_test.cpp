/*
 * phi_apriori_test.cpp
 *
 *  Created on: 2013-8-11
 *      Author: Yan Shankai
 */

#include <time.h>
#include <stdlib.h>
#include "libyskalgrthms/math/digit_util.h"
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/association/extractor/mpi_doc_text_extractor.h"
#include "libyskdmu/association/phi_apriori.h"

static clock_t start, finish;
static time_t start_t, finish_t;
static double duration, duration_t;

void test_phi_apriori_doc(int argc, char *argv[],
		unsigned int max_itemset_size = 2, double minsup = 0.05,
		double minconf = 0.4) {
	int pid, numprocs;
	int root_pid = 0;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);

	if (pid == root_pid) {
		printf(
				"**********PHI-Apriori with Doc Data testing start testing**********\n");
		printf(
				"Max itemset's size is: %u; minimum support is: %f; minimum confidence is: %f\n",
				max_itemset_size, minsup, minconf);
		start = clock();
		start_t = time(NULL);
	}
	bool print_item_detial = false;
	bool print_index = false;
//	bool print_itemset = false;
	bool print_frequent_itemsets = false;
	bool print_assoc_rules = false;

	vector<KItemsets> frequent_itemsets;
	frequent_itemsets.reserve(max_itemset_size);
	vector<AssociationRule<DocItemDetail> > assoc_rules;
	MPIDocTextExtractor doc_text(MPI_COMM_WORLD );

	ParallelHiApriori<DocItem, DocItemDetail, DocTextRecordInfo> phi_apriori =
			ParallelHiApriori<DocItem, DocItemDetail, DocTextRecordInfo>(
					MPI_COMM_WORLD );

	phi_apriori.init(max_itemset_size, minsup, minconf);
	phi_apriori.set_frequent_itemsets(&frequent_itemsets);
	phi_apriori.set_assoc_rules(&assoc_rules);
	phi_apriori.set_extractor(&doc_text);
	phi_apriori.enable_log(print_frequent_itemsets);

	bool succeed = true;
	succeed &= phi_apriori.phi_apriori();
	succeed &= phi_apriori.phi_genrules();

	unsigned int frq_itemset_nums[frequent_itemsets.size()];
	for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
		unsigned int frq_itemset_num =
				frequent_itemsets[i].get_itemsets().size();
		unsigned int total_frq_itemset_num;
		MPI_Reduce(&frq_itemset_num, &total_frq_itemset_num, 1, MPI_UNSIGNED,
				MPI_SUM, root_pid, MPI_COMM_WORLD );
		frq_itemset_nums[i] = total_frq_itemset_num;
	}

	if (pid == root_pid) {
		printf(succeed ? "PHI-Apriori Succeed!\n" : "PHI-Apriori Faild!\n");

		if (print_index) {
			printf("Index:\n");
			IndexHead **hash_table =
					(IndexHead **) phi_apriori.m_item_index->get_hash_table();
			const char *identifier = NULL;
			for (unsigned int i = 0; i < 1000; i++) {
				if (hash_table[i] != NULL) {
					identifier = phi_apriori.m_item_details[ysk_atoi(
							hash_table[i]->key_info,
							strlen(hash_table[i]->key_info))].m_identifier;
					printf(
							"slot: %u\thashcode: %u\tkey: %s------Record index: ",
							i,
							phi_apriori.m_item_index->hashfunc(identifier,
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

		if (print_item_detial) {
			printf("ItemDetail:\n{ ");
			for (unsigned int i = 0; i < phi_apriori.m_item_details.size();
					i++) {
				printf("[%u]%s, ", i,
						phi_apriori.m_item_details[i].m_identifier);
			}
			printf("}\n");
		}

//		if (print_itemset) {
//			for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
//				const map<vector<unsigned int>, unsigned int>& k_itemsets =
//						frequent_itemsets[i].get_itemsets();
//				printf("%u-itemset(number: %u):\n", i + 1, k_itemsets.size());
//				for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
//						k_itemsets.begin(); iter != k_itemsets.end(); iter++) {
//					printf("{");
//					for (unsigned int j = 0; j < iter->first.size(); j++) {
//						printf("%s,",
//								(phi_apriori.m_item_details)[iter->first[j]].m_identifier);
//					}
//					printf("}\n");
//				}
//				printf("\n");
//			}
//		}

		for (unsigned int i = 0; i < frequent_itemsets.size(); i++) {
			printf("Frequent %u-itemsets size is: %u\n", i + 1,
					frq_itemset_nums[i]);
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
				printf("Association Rules[%u]: %s ===> %s ; confidence: %f\n",
						i, condition.c_str(), consequent.c_str(),
						assoc_rules[i].confidence);
			}
		}

		finish = clock();
		finish_t = time(NULL);
		duration = (double) (finish - start) / (CLOCKS_PER_SEC);
		duration_t = difftime(finish_t, start_t);
		printf("Record amount: %u\n", phi_apriori.m_record_infos.size());
		printf(
				"PHI-Apriori with Doc Data testing duaration: %f(seconds) or %f(seconds)\n",
				duration, duration_t);
		printf(
				"**********PHI-Apriori with Doc Data finish testing**********\n\n");
	}
	MPI_Finalize();
}
