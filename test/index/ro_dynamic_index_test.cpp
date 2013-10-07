/*
 * ro_dynamic_index_test.cpp
 *
 *  Created on: 2013-10-5
 *      Author: Yan Shankai
 */

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/index/dynamic_hash_index.h"
#include "libyskdmu/index/ro_dhi_data.h"
#include "libyskdmu/index/ro_dynamic_hash_index.h"

static clock_t start, finish;
static float duration;

void test_ro_dynamic_hash_index() {
	printf("**********ReadOnlyDynamicHashIndex start testing**********\n");
	start = clock();
	unsigned int record_num = 15;
	unsigned int term_num_per_record = 5;
	unsigned int intersect_iden_num = 2;
	bool print_index = true;
	bool print_intersect_record_num = true;
	const char* identifiers[11] = { "chicken", "dog", "bumblebee", "cat",
			"hello kitty", "snoopy", "sheldon", "penny", "saber", "archer",
			"T-Pat" };
	srand((unsigned int) time(NULL));
	RODynamicHashIndexData ro_index_data;
	DynamicHashIndex index = DynamicHashIndex(4, 2);
	const char *identifier = NULL;
	unsigned int iden_index, hashcode;
	for (unsigned int i = 0; i < record_num * term_num_per_record; i++) {
		iden_index = rand() % 11;
		identifier = identifiers[iden_index];
		char* iden_index_str = itoa(iden_index);
		hashcode = index.insert(identifier, strlen(identifier), iden_index_str,
				i / term_num_per_record);
		delete[] iden_index_str;
	}

	if (print_index) {
		index.print_index();
	}

	ro_index_data.build(&index);
	unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
			*l2_index_size;
	unsigned char* l2_index;
	ro_index_data.fill_memeber_data(&d, &data, &data_size, &l1_index,
			&l1_index_size, &l2_index, &l2_index_size);
	RODynamicHashIndex ro_index(d, data, data_size, l1_index, l1_index_size,
			l2_index, l2_index_size, simple_hash);

	const char* target_identifier = identifiers[rand() % 11];
	unsigned int records[record_num];
	printf("Finding records of %s...", target_identifier);
	unsigned int target_record_num = ro_index.find_record(records,
			target_identifier, strlen(target_identifier));
	if (target_record_num == 0) {
		printf("is failed!");
	}
	for (unsigned int i = 0; i < target_record_num; i++) {
		printf("%u, ", records[i]);
	}
	printf("\n");

	if (print_intersect_record_num) {
		printf("Intersect record id of ");
		for (unsigned int i = 0; i < intersect_iden_num; i++) {
			printf("%s, ", identifiers[i]);
		}
		printf("is: ");
		unsigned int *records = ro_index.get_intersect_records(identifiers,
				intersect_iden_num);
		for (unsigned int i = 1; i <= records[0]; i++) {
			printf("%u, ", records[i]);
		}
		printf("\n");
		delete[] records;
	}

	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf("ReadOnlyDynamicHashIndex testing duaration: %f(secs)\n", duration);
	printf("**********ReadOnlyDynamicHashIndex finish testing**********\n\n");
}
