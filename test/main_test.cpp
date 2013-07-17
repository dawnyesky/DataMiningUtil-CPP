/*
 * main_test.cpp
 *
 *  Created on: 2011-11-25
 *      Author: Yan Shankai
 */

#include <stdio.h>

extern void test_is_subset();

extern void test_triangle_matrix();
extern void test_hash_table_counter();

extern void test_open_hash_index();
extern void test_mpi_d_hash_index(int argc, char *argv[]);

extern void test_trade_x_extractor();
extern void test_doc_text_extractor();

extern void test_apriori_xml(unsigned int max_itemset_size = 3, double minsup =
		0.6, double minconf = 0.4);
extern void test_apriori_doc(unsigned int max_itemset_size = 2, double minsup =
		0.05, double minconf = 0.4);
extern void test_apriori_strvv(unsigned int max_itemset_size = 3,
		double minsup = 0.6, double minconf = 0.4);

extern void test_hi_apriori_xml(unsigned int max_itemset_size = 3,
		double minsup = 0.6, double minconf = 0.4);
extern void test_hi_apriori_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.05, double minconf = 0.4);

extern void test_fp_growth_xml(unsigned int max_itemset_size = 3,
		double minsup = 0.6, double minconf = 0.4);
extern void test_fp_growth_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.05, double minconf = 0.4);

extern void test_dhp_xml(unsigned int max_itemset_size = 3, double minsup = 0.6,
		double minconf = 0.4);
extern void test_dhp_doc(unsigned int max_itemset_size = 2,
		double minsup = 0.05, double minconf = 0.4);

extern void test_combine_algorithms(unsigned int max_itemset_size = 2,
		double minsup = 0.2, double minconf = 0.4);

extern void test_cf_ub();

extern void test_cf_ub_wds();

extern void test_cf_ub(int argc, char *argv[]);

extern void test_cf_ib_wds();

extern void test_cf_ib(int argc, char *argv[]);

extern void test_cf_uib(double ib_coe, double ib_sim_thres,
		double ub_sim_thres);

extern void find_best_coe();

extern void test_cf_assoc_wds();

int main(int argc, char *argv[]) {
//	test_is_subset();
//	test_triangle_matrix();
//	test_hash_table_counter();
//	test_open_hash_index();
	test_mpi_d_hash_index(argc, argv);
//	test_trade_x_extractor();
//	test_doc_text_extractor();
//	test_apriori_xml();
//	test_apriori_strvv();
//	test_apriori_doc();
//	test_fp_growth_xml();
//	test_fp_growth_doc();
//	test_dhp_xml();
//	test_dhp_doc();
//	test_hi_apriori_xml();
//	test_hi_apriori_doc();
//	test_combine_algorithms();
//	test_cf();
//	test_cf_ub_wds();
//	test_cf_ub(argc, argv);
//	test_cf_ib_wds();
//	test_cf_ib(argc, argv);
//	test_cf_uib(0.67, 0.03, 0.02);
//	find_best_coe();
//	test_cf_assoc_wds();
	return 0;
}
