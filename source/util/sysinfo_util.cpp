/*
 * sysinfo_util.cpp
 *
 *  Created on: 2013-9-6
 *      Author: Yan Shankai
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "libyskdmu/util/sysinfo_util.h"

const char* SysInfoUtil::units[] = { "KB", "MB", "GB" };

unsigned int SysInfoUtil::get_proc_mem_usage(pid_t pid, unsigned int unit) {
	unsigned int result = 0;
	FILE *fp;
	char buf[32];
	char line[256];
	memset(buf, 0, sizeof(buf));
	memset(line, 0, sizeof(line));

	sprintf(buf, "/proc/%d/status", pid);
	if ((fp = fopen(buf, "r")) == NULL) {
		return 0;
	}

	while (line == fgets(line, sizeof(line), fp)) {
		if (strstr(line, "VmRSS:")) {
			sscanf(line, "VmRSS:\t%8u kB\n", &result);
			break;
		}
	}

	fclose(fp);
	switch (unit) {
	case (0):
		break;
	case (1):
		result /= 1024;
		break;
	case (2):
		result /= 1048576;
		break;
	default:
		printf("Memory size unit is set uncorrected!\n");
		break;
	}
	return result;
}

unsigned int SysInfoUtil::get_cur_proc_mem_usage(unsigned int unit) {
	pid_t pid = getpid();
	return get_proc_mem_usage(pid, unit);
}
