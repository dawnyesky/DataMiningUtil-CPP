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
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/index/mpi_d_hash_index.h"

static clock_t start, finish;
static float duration;

void test_mpi_d_hash_index(int argc, char *argv[], unsigned int record_num = 15,
		unsigned int term_num_per_record = 5) {
	int pid, numprocs;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);
	printf(
			"**********MPIDistributedHashIndex start testing in processor%i**********\n",
			pid);
	start = clock();
	unsigned int intersect_iden_num = 2;
	bool print_index = true;
	bool print_intersect_record_num = false;
	const char* identifiers[11] = { "chicken", "dog", "bumblebee", "cat",
			"hello kitty", "snoopy", "sheldon", "penny", "saber", "archer",
			"T-Pat" };

	MPIDHashIndex index = MPIDHashIndex(MPI_COMM_WORLD );

	const char *identifier = NULL;
	unsigned int iden_index, hashcode;
	srand((unsigned int) time(NULL) * pid);
	for (unsigned int i = 0; i < record_num * term_num_per_record; i++) {
		iden_index = rand() % 11;
		identifier = identifiers[iden_index];
		char* iden_index_str = itoa(iden_index);
		hashcode = index.insert(identifier, strlen(identifier), iden_index_str,
				pid * record_num + i / term_num_per_record);
		delete[] iden_index_str;
	}

#ifdef __DEBUG__
	if (print_index) {
		printf("Start to print the index before synchronize:\n");
		index.print_index();
		printf("End to print the index before synchronize:\n");
	}
#endif

	index.synchronize();

#ifdef __DEBUG__
	if (print_index) {
		printf("The index after synchronize:\n");
		index.print_index();
	}
#endif

	index.consolidate();

#ifdef __DEBUG__
	if (print_index) {
		printf("The index after consolidate:\n");
		index.print_index();
	}
#endif

	if (print_intersect_record_num && pid == 0) {
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
	printf(
			"MPIDistributedHashIndex testing in processor%i duaration: %f(secs)\n",
			pid, duration);
	printf(
			"**********MPIDistributedHashIndex finish testing in processor%i**********\n\n",
			pid);
	MPI_Finalize();
}
