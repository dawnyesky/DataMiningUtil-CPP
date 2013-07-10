/*
 * index_test.cpp
 *
 *  Created on: 2011-12-3
 *      Author: Yan Shankai
 */

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libyskdmu/index/open_hash_index.h"

using namespace std;

static clock_t start, finish;
static float duration;

void test_open_hash_index() {
	printf("**********OpenHashIndex start testing**********\n");
	start = clock();
	unsigned int record_num = 10;
	unsigned int term_num_per_record = 10;
	unsigned int table_size = 30;
	unsigned int intersect_iden_num = 2;
	bool print_index = true;
	bool print_intersect_record_num = true;
	const char* identifiers[11] = { "chicken", "dog", "bumblebee", "cat",
			"hello kitty", "snoopy", "sheldon", "penny", "saber", "archer",
			"T-Pat" };
	srand((unsigned int) time(NULL));
	OpenHashIndex index = OpenHashIndex(table_size);
	const char *identifier = NULL;
	unsigned int iden_index, hashcode;
	for (unsigned int i = 0; i < record_num * term_num_per_record; i++) {
		iden_index = rand() % 11;
		identifier = identifiers[iden_index];
		hashcode = index.insert(identifier, strlen(identifier), iden_index,
				i / term_num_per_record);
	}

	if (print_index) {
		IndexHead **hash_table = (IndexHead **) index.get_hash_table();
		for (unsigned int i = 0; i < table_size; i++) {
			if (hash_table[i] != NULL) {
				identifier = identifiers[hash_table[i]->key_info];
				printf(
						"slot: %u\thashcode: %u\tkey: %s------Record numbers: %u------Record index: ",
						i, index.hashfunc(identifier, strlen(identifier)),
						identifier, hash_table[i]->index_item_num);

				//				IndexItem *p = hash_table[i]->inverted_index;
				//				while (p != NULL) {
				//					printf("%u, ", p->record_id);
				//					p = p->next;
				//				}

				unsigned int records[index.get_mark_record_num(identifier,
						strlen(identifier))];
				unsigned int num = index.find_record(records, identifier,
						strlen(identifier));
				for (unsigned int j = 0; j < num; j++) {
					printf("%u, ", records[j]);
				}

				printf("\n");
			}
		}
	}

	if (print_intersect_record_num) {
		printf("Intersect record id of ");
		for (unsigned int i = 0; i < intersect_iden_num; i++) {
			printf("%s, ", identifiers[i]);
		}
		printf("is: ");
		unsigned int *records = index.get_intersect_records(identifiers,
				intersect_iden_num);
		for (unsigned int i = 1; i <= records[0]; i++) {
			printf("%u, ", records[i]);
		}
		printf("\n");
		delete[] records;
	}

	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf("OpenHashIndex testing duaration: %f(secs)\n", duration);
	printf("**********OpenHashIndex finish testing**********\n\n");
}
