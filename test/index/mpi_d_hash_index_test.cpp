/*
 * mpi_d_hash_index_test.cpp
 *
 *  Created on: 2013-7-15
 *      Author: Yan Shankai
 */

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "libyskdmu/index/mpi_d_hash_index.h"

static clock_t start, finish;
static float duration;

void test_mpi_d_hash_index(int argc, char *argv[]) {
	int pid, numproc;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numproc);
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);
	printf(
			"**********MPIDistributedHashIndex start testing in processor%i**********\n",
			pid);
	start = clock();
	unsigned int record_num = 15;
	unsigned int term_num_per_record = 5;
//	unsigned int intersect_iden_num = 2;
	bool print_index = true;
//	bool print_inteFrsect_record_num = true;
	const char* identifiers[11] = { "chicken", "dog", "bumblebee", "cat",
			"hello kitty", "snoopy", "sheldon", "penny", "saber", "archer",
			"T-Pat" };

	MPIDHashIndex index = MPIDHashIndex(MPI_COMM_WORLD);
	index.init();

	const char *identifier = NULL;
	unsigned int iden_index, hashcode;
	srand((unsigned int) time(NULL));
	for (unsigned int i = 0; i < record_num * term_num_per_record; i++) {
		iden_index = rand() % 11;
		identifier = identifiers[iden_index];
		hashcode = index.insert(identifier, strlen(identifier), iden_index,
				i / term_num_per_record);
	}

	if (print_index) {
		printf("Start to print the index before synchronize:\n");
		Catalog* catalogs = (Catalog*) index.get_hash_table();
		for (unsigned int i = 0;
				i < (unsigned int) pow(2, index.get_global_deep()); i++) {
			if (catalogs[i].bucket != NULL) {
				vector<IndexHead> elements = catalogs[i].bucket->elements;
				for (unsigned int j = 0; j < elements.size(); j++) {
					identifier = identifiers[elements[j].key_info];
					printf(
							"catalog: %u\thashcode: %u\tkey: %s------Record numbers: %u------Record index: ",
							i, index.hashfunc(identifier, strlen(identifier)),
							identifier, elements[j].index_item_num);
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
		printf("End to print the index before synchronize:\n");
	}

	index.synchronize();

	if (print_index) {
		printf("The index after synchronize:\n");
		Catalog* catalogs = (Catalog*) index.get_hash_table();
		for (unsigned int i = 0;
				i < (unsigned int) pow(2, index.get_global_deep()); i++) {
			if (catalogs[i].bucket != NULL) {
				vector<IndexHead> elements = catalogs[i].bucket->elements;
				for (unsigned int j = 0; j < elements.size(); j++) {
					identifier = identifiers[elements[j].key_info];
					printf(
							"catalog: %u\thashcode: %u\tkey: %s------Record numbers: %u------Record index: ",
							i, index.hashfunc(identifier, strlen(identifier)),
							identifier, elements[j].index_item_num);
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
	}

//	if (print_intersect_record_num) {
//		printf("Intersect record id of ");
//		for (unsigned int i = 0; i < intersect_iden_num; i++) {
//			printf("%s, ", identifiers[i]);
//		}
//		printf("is: ");
//		unsigned int *records = index.get_intersect_records(identifiers,
//				intersect_iden_num);
//		for (unsigned int i = 1; i <= records[0]; i++) {
//			printf("%u, ", records[i]);
//		}
//		printf("\n");
//		delete[] records;
//	}

	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf(
			"MPIDistributedHashIndex testing in processor%i duaration: %f(secs)\n",
			pid, duration);
	printf(
			"**********MPIDistributedHashIndex finish testing in processor%i**********\n\n",
			pid);
	MPI_Finalize();
}
