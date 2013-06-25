/*
 * log_util.h
 *
 *  Created on: 2012-10-22
 *      Author: "Yan Shankai"
 */

#ifndef LOG_UTIL_H_
#define LOG_UTIL_H_

#include <map>
#ifndef __MINGW32__
#include <log4cpp/PropertyConfigurator.hh>
#include <log4cpp/Category.hh>
#include <log4cpp/FileAppender.hh>
#endif

using namespace std;

class LogInstance {
public:
	LogInstance(const char* identifier);
	bool isDebugEnabled();
	bool isInfoEnabled();
	bool isWarnEnabled();
	bool isErrorEnabled();
	void debug(const char* string_format, ...);
	void info(const char* string_format, ...);
	void warn(const char* string_format, ...);
	void error(const char* string_format, ...);
	const char* m_identifier;

private:
#ifndef __MINGW32__
	log4cpp::Category* log;
#endif
};

class LogUtil {
public:
	LogUtil();
	virtual ~LogUtil();
	static LogUtil* get_instance();
	static void destroy_instance();
	LogInstance* get_log_instance(const char* identifier);

private:
	static LogUtil* m_instance;
	static bool is_configured;
	map<const char*, LogInstance*> m_log_instances;
};

#endif /* LOG_UTIL_H_ */
