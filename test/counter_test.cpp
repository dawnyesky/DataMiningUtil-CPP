/*
 * counter_test.cpp
 *
 *  Created on: 2011-11-25
 *      Author: Yan Shankai
 */

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include "libyskalgrthms/math/arrange_combine.h"
#include "libyskdmu/counter/array_triangle_matrix.h"
#include "libyskdmu/counter/hash_table_counter.h"
#include "libyskdmu/counter/map_triangle_matrix.h"
#include "libyskdmu/counter/dynamic_hash_counter.h"

using namespace std;

static clock_t start, finish;
static float duration;

void test_triangle_matrix() {
	printf("**********TrangleMatrixCounter start testing**********\n");
	start = clock();
	unsigned int space_size = 10;
	unsigned int record_num = 40;
	bool print_matrix = true;

	if (record_num > combine(space_size, 2)) {
		printf("the parameter record_num is too large to let it go!");
		exit(-1);
	}

	srand((unsigned int) time(NULL));
	Counter *matrix = new TriangleMatrix(space_size);
	unsigned int data[record_num][2];
	for (unsigned int i = 0; i < record_num; i++) {
		for (unsigned int j = 0; j < 2; j++) {
			data[i][j] = rand() % space_size;
		}
		matrix->count(data[i]);
		//		printf("[%u](%u, %u)\n", i, data[i][0], data[i][1]);
	}

	if (print_matrix) {
		unsigned int item[2];
		for (item[0] = 0; item[0] < space_size; ++item[0]) {
			for (item[1] = 0; item[1] < space_size; ++item[1]) {
				if (item[0] == item[1]) { //对角线元素
					printf("%i\t", 999);
				} else {
					printf("%u\t", matrix->get_count(item));
				}
			}
			printf("\n");
		}
	}
	delete matrix;
	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf("TrangleMatrixCounter testing duaration: %f(secs)\n", duration);
	printf("**********TrangleMatrixCounter finish testing**********\n\n");
}

void test_hash_table_counter() {
	printf("**********HashTableCounter start testing**********\n");
	start = clock();
	unsigned int space_size = 1000;
	unsigned int dimension = 2;
	unsigned int record_num = 1000;
	bool print_matrix = false;

	if (record_num > combine(space_size, dimension)) {
		printf("the parameter record_num is too large to let it go!");
		exit(-1);
	}

	srand((unsigned int) time(NULL));
	Counter *matrix = new HashTableCounter(space_size, dimension);
	unsigned int data[record_num][dimension];
	for (unsigned int i = 0; i < record_num; i++) {
		for (unsigned int j = 0; j < dimension; j++) {
			data[i][j] = rand() % space_size;
		}
		matrix->count(data[i]);
//				printf("[%u](%u, %u)\n", i, data[i][0], data[i][1]);
	}

	if (dimension == 2 && print_matrix) {
		unsigned int item[2];
		for (item[0] = 0; item[0] < space_size; ++item[0]) {
			for (item[1] = 0; item[1] < space_size; ++item[1]) {
				if (item[0] == item[1]) { //对角线元素
					printf("%i\t", 999);
				} else {
					printf("%u\t", matrix->get_count(item));
				}
			}
			printf("\n");
		}
	}
	delete matrix;
	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf("HashTableCounter testing duaration: %f(secs)\n", duration);
	printf("**********HashTableCounter finish testing**********\n\n");
}

void test_dynamic_hash_counter() {
	printf("**********DynamicHashCounter start testing**********\n");
	start = clock();
	unsigned int space_size = 1000;
	unsigned int dimension = 2;
	unsigned int record_num = 1000;
	bool print_matrix = false;

	if (record_num > combine(space_size, dimension)) {
		printf("the parameter record_num is too large to let it go!");
		exit(-1);
	}

	srand((unsigned int) time(NULL));
	Counter *matrix = new DynamicHashCounter(space_size, dimension);
	unsigned int data[record_num][dimension];
	for (unsigned int i = 0; i < record_num; i++) {
		for (unsigned int j = 0; j < dimension; j++) {
			data[i][j] = rand() % space_size;
		}
		matrix->count(data[i]);
//				printf("[%u](%u, %u)\n", i, data[i][0], data[i][1]);
	}

	if (dimension == 2 && print_matrix) {
		unsigned int item[2];
		for (item[0] = 0; item[0] < space_size; ++item[0]) {
			for (item[1] = 0; item[1] < space_size; ++item[1]) {
				if (item[0] == item[1]) { //对角线元素
					printf("%i\t", 999);
				} else {
					printf("%u\t", matrix->get_count(item));
				}
			}
			printf("\n");
		}
	}
	delete matrix;
	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf("DynamicHashCounter testing duaration: %f(secs)\n", duration);
	printf("**********DynamicHashCounter finish testing**********\n\n");
}
