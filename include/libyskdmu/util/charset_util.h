/*
 * charset_util.h
 *
 *  Created on: 2011-12-10
 *      Author: Yan Shankai
 */

#ifndef CHARSET_UTIL_H_
#define CHARSET_UTIL_H_

#include <vector>
using namespace std;

int convert_charset(const char *to_charset, const char *from_charset,
		char *outbuf, size_t outlen, char* inbuf, size_t inlen);

#endif /* CHARSET_UTIL_H_ */
