/*
 * set_util_test.cpp
 *
 *  Created on: 2012-5-25
 *      Author: "Yan Shankai"
 */

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include "libyskdmu/util/set_util.h"

using namespace std;

static clock_t start, finish;
static float duration;

void test_is_subset() {
	printf("**********Function is_subset start testing**********\n");
		start = clock();
		int sub[3] = {3, 5, 7};
		int super[7] = {1, 2, 3, 4, 5, 6, 7};
		bool succeed = is_subset<int>(vector<int>(super, super + 7), vector<int>(sub, sub + 3));
		printf("{3, 5, 7} is subset of {1, 2, 3, 4, 5, 6, 7}: %s\n", succeed ? "True" : "False");
		finish = clock();
		duration = (float) (finish - start) / (CLOCKS_PER_SEC);
		printf("Function is_subset testing duaration: %f(secs)\n", duration);
		printf("**********Function is_subset finish testing**********\n\n");
}
