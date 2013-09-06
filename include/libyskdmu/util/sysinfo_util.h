/*
 * sysinfo_util.h
 *
 *  Created on: 2013-9-6
 *      Author: Yan Shankai
 */

#ifndef SYSINFO_UTIL_H_
#define SYSINFO_UTIL_H_

#include <sys/types.h>

class SysInfoUtil {
public:
	static unsigned int get_proc_mem_usage(pid_t pid, unsigned int unit = 0);
	static unsigned int get_cur_proc_mem_usage(unsigned int unit = 0);

private:
	const static char* units[];
	const static unsigned int MEM_UNIT_KB = 0;
	const static unsigned int MEM_UNIT_MB = 1;
	const static unsigned int MEM_UNIT_GB = 2;
};

#endif /* SYSINFO_UTIL_H_ */
