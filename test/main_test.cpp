/*
 * main_test.cpp
 *
 *  Created on: 2011-11-25
 *      Author: Yan Shankai
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

const static char* IS_SUBSET = "is_subset";
extern void test_is_subset();

const static char* TRIANGLE_MATRIX = "triangle_matrix";
extern void test_triangle_matrix();
const static char* HASH_TABLE_COUNTER = "hash_table_counter";
extern void test_hash_table_counter();

const static char* OPEN_HASH_INDEX = "open_hash_index";
extern void test_open_hash_index();
const static char* DYNAMIC_HASH_INDEX = "dynamic_hash_index";
extern void test_dynamic_hash_index();
const static char* MPI_D_HASH_INDEX = "mpi_d_hash_index";
extern void test_mpi_d_hash_index(int argc, char *argv[],
		unsigned int record_num = 15, unsigned int term_num_per_record = 5);

const static char* TRADE_X_EXTRACTOR = "trade_x_extractor";
extern void test_trade_x_extractor();
const static char* DOC_TEXT_EXTRACTOR = "doc_text_extractor";
extern void test_doc_text_extractor();

const static char* APRIORI_XML = "apriori_xml";
extern void test_apriori_xml(unsigned int max_itemset_size = 3, double minsup =
		0.6, double minconf = 0.4);
const static char* APRIORI_DOC = "apriori_doc";
extern void test_apriori_doc(unsigned int max_itemset_size = 2, double minsup =
		0.05, double minconf = 0.4);
const static char* APRIORI_STRVV = "apriori_strvv";
extern void test_apriori_strvv(unsigned int max_itemset_size = 3,
		double minsup = 0.6, double minconf = 0.4);

const static char* HI_APRIORI_XML = "hi_apriori_xml";
extern void test_hi_apriori_xml(unsigned int max_itemset_size = 3,
		double minsup = 0.6, double minconf = 0.4);
const static char* HI_APIORI_DOC = "hi_apriori_doc";
extern void test_hi_apriori_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.05, double minconf = 0.4);

const static char* FP_GROWTH_XML = "fp_growth_xml";
extern void test_fp_growth_xml(unsigned int max_itemset_size = 3,
		double minsup = 0.6, double minconf = 0.4);
const static char* FP_GROWTH_DOC = "fp_growth_doc";
extern void test_fp_growth_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.05, double minconf = 0.4);

const static char* DHP_XML = "dhp_xml";
extern void test_dhp_xml(unsigned int max_itemset_size = 3, double minsup = 0.6,
		double minconf = 0.4);
const static char* DHP_DOC = "dhp_doc";
extern void test_dhp_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.05, double minconf = 0.4);

const static char* COMBINE_ALGORITHMS = "combine_algorithms";
extern void test_combine_algorithms(unsigned int max_itemset_size = 2,
		double minsup = 0.2, double minconf = 0.4);

const static char* PHI_APRIORI_DOC = "phi_apriori_doc";
extern void test_phi_apriori_doc(int argc, char *argv[],
		unsigned int max_itemset_size = 2, double minsup = 0.05,
		double minconf = 0.4);

const static char* CF_UB = "cf_ub";
extern void test_cf_ub();
const static char* CF_UB_WDS = "cf_ub_wds";
extern void test_cf_ub_wds();
const static char* CF_UB_ARGS = "cf_ub_args";
extern void test_cf_ub(int argc, char *argv[]);
const static char* CF_IB_WDS = "cf_ib_wds";
extern void test_cf_ib_wds();
const static char* CF_IB = "cf_ib";
extern void test_cf_ib(int argc, char *argv[]);
const static char* CF_UIB = "cf_uib";
extern void test_cf_uib(double ib_coe, double ib_sim_thres,
		double ub_sim_thres);
const static char* FIND_BEST_COE = "find_best_one";
extern void find_best_coe();
const static char* CF_ASSOC_WDS = "cf_assoc_wds";
extern void test_cf_assoc_wds();

int main(int argc, char *argv[]) {
	if (argc < 2) {
		printf("usage: %s test_case [args]", argv[0]);
	}
	if (strcmp(argv[1], IS_SUBSET) == 0) {
		test_is_subset();
	} else if (strcmp(argv[1], TRIANGLE_MATRIX) == 0) {
		test_triangle_matrix();
	} else if (strcmp(argv[1], HASH_TABLE_COUNTER) == 0) {
		test_hash_table_counter();
	} else if (strcmp(argv[1], OPEN_HASH_INDEX) == 0) {
		test_open_hash_index();
	} else if (strcmp(argv[1], DYNAMIC_HASH_INDEX) == 0) {
		test_dynamic_hash_index();
	} else if (strcmp(argv[1], MPI_D_HASH_INDEX) == 0) {
		if (argc < 4) {
			printf("usage: %s %s record_num term_num_per_record", argv[0],
					MPI_D_HASH_INDEX);
		}
		test_mpi_d_hash_index(argc, argv, atoi(argv[2]), atoi(argv[3]));
	} else if (strcmp(argv[1], TRADE_X_EXTRACTOR) == 0) {
		test_trade_x_extractor();
	} else if (strcmp(argv[1], DOC_TEXT_EXTRACTOR) == 0) {
		test_doc_text_extractor();
	} else if (strcmp(argv[1], APRIORI_XML) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					APRIORI_XML);
		}
		test_apriori_xml(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], APRIORI_DOC) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					APRIORI_DOC);
		}
		test_apriori_doc(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], APRIORI_STRVV) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					APRIORI_STRVV);
		}
		test_apriori_strvv(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], HI_APRIORI_XML) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					HI_APRIORI_XML);
		}
		test_hi_apriori_xml(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], HI_APIORI_DOC) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					HI_APIORI_DOC);
		}
		test_hi_apriori_doc(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], FP_GROWTH_XML) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					FP_GROWTH_XML);
		}
		test_fp_growth_xml(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], FP_GROWTH_DOC) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					FP_GROWTH_DOC);
		}
		test_fp_growth_doc(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], DHP_XML) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					DHP_XML);
		}
		test_dhp_xml(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], DHP_DOC) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					DHP_DOC);
		}
		test_dhp_doc(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], COMBINE_ALGORITHMS) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					COMBINE_ALGORITHMS);
		}
		test_combine_algorithms(atoi(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], PHI_APRIORI_DOC) == 0) {
		if (argc < 5) {
			printf("usage: %s %s max_itemset_size minsup minconf", argv[0],
					PHI_APRIORI_DOC);
		}
		test_phi_apriori_doc(argc, argv, atoi(argv[2]), atof(argv[3]),
				atof(argv[4]));
	} else if (strcmp(argv[1], CF_UB) == 0) {
		test_cf_ub();
	} else if (strcmp(argv[1], CF_UB_WDS) == 0) {
		test_cf_ub_wds();
	} else if (strcmp(argv[1], CF_UB_ARGS) == 0) {
		test_cf_ub(argc, argv);
	} else if (strcmp(argv[1], CF_IB_WDS) == 0) {
		test_cf_ib_wds();
	} else if (strcmp(argv[1], CF_IB) == 0) {
		test_cf_ib(argc, argv);
	} else if (strcmp(argv[1], CF_UIB) == 0) {
		if (argc < 5) {
			printf("usage: %s %s ib_coe ib_sim_thres ub_sim_thres", argv[0],
					CF_UIB);
		}
		test_cf_uib(atof(argv[2]), atof(argv[3]), atof(argv[4]));
	} else if (strcmp(argv[1], FIND_BEST_COE) == 0) {
		find_best_coe();
	} else if (strcmp(argv[1], CF_ASSOC_WDS) == 0) {
		test_cf_assoc_wds();
	} else {
		printf("test case \'%s\' not found\nusage: %s test_case args", argv[1],
				argv[0]);
	}
	return 0;
}
