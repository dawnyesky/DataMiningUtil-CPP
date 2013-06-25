/*
 * charset_util.cpp
 *
 *  Created on: 2011-12-10
 *      Author: Yan Shankai
 */

#include <string.h>
#include <iconv.h>
#include "libyskdmu/util/charset_util.h"

int convert_charset(const char *to_charset, const char *from_charset,
		char *outbuf, size_t outlen, char* inbuf, size_t inlen) {
	iconv_t cd;
	char **inptr = &inbuf;
	char **outptr = &outbuf;

	memset(outbuf, 0, outlen);
	cd = iconv_open(to_charset, from_charset);
	if (-1ul == iconv(cd, inptr, &inlen, outptr, &outlen))
		return -1;
	if (-1 == iconv_close(cd))
		return -1;
	return 0;
}
