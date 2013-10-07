/*
 * log_util.cpp
 *
 *  Created on: 2012-10-22
 *      Author: "Yan Shankai"
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include "libyskdmu/util/conf_util.h"
#include "libyskdmu/util/log_util.h"

LogUtil* LogUtil::m_instance = NULL;
LogUtil::__Garbage__ LogUtil::m_garbage;
bool LogUtil::is_configured = false;

LogUtil::LogUtil() {
	try {
		ConfUtil* conf_util = ConfUtil::get_instance();
		ConfInstance* conf_instance = conf_util->get_conf_instance("global");
		const char* conf_root_dir = conf_instance->get_configuration(
				"ROOT_DIR");
		const char* log4cpp_conf_path = conf_instance->get_configuration(
				"LOG4CPP_CONF_PATH");
		char log4cpp_conf[strlen(conf_root_dir) + strlen(log4cpp_conf_path) + 1];
		strcpy(log4cpp_conf, conf_root_dir);
		strcat(log4cpp_conf, log4cpp_conf_path);
		log4cpp::PropertyConfigurator::configure(log4cpp_conf);
		is_configured = true;
	} catch (log4cpp::ConfigureFailure& f) {
		printf("Configure Problem: %s\n", f.what());
	}
}

LogUtil::~LogUtil() {
	for (map<const char*, LogInstance*>::const_iterator iter =
			m_log_instances.begin(); iter != m_log_instances.end(); iter++) {
		if (iter->second != NULL) {
			delete iter->second;
		}
	}
}

LogUtil* LogUtil::get_instance() {
	if (m_instance != NULL) {
		return m_instance;
	} else {
		m_instance = new LogUtil();
		return m_instance;
	}
}

void LogUtil::destroy_instance() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}

LogInstance* LogUtil::get_log_instance(const char* identifier) {
	for (map<const char*, LogInstance*>::const_iterator iter =
			m_log_instances.begin(); iter != m_log_instances.end(); iter++) {
		if (strcmp(iter->first, identifier) == 0) {
			return iter->second;
		}
	}
	LogInstance* log_ptr = new LogInstance(identifier);
	m_log_instances.insert(
			pair<const char*, LogInstance*>(identifier, log_ptr));
	return log_ptr;
}

LogInstance::LogInstance(const char* identifier) {
	log = &log4cpp::Category::getInstance(std::string(identifier));
	m_identifier = identifier;
}

bool LogInstance::isDebugEnabled() {
	return log->isDebugEnabled();
}

bool LogInstance::isInfoEnabled() {
	return log->isInfoEnabled();
}

bool LogInstance::isWarnEnabled() {
	return log->isWarnEnabled();
}

bool LogInstance::isErrorEnabled() {
	return log->isErrorEnabled();
}

void LogInstance::debug(const char* string_format, ...) {
	char buf[1024];
	va_list args;
	va_start(args, string_format);
	vsnprintf(buf, 1024, string_format, args);
	log->debug(buf);
	va_end(args);
}

void LogInstance::info(const char* string_format, ...) {
	char buf[1024];
	va_list args;
	va_start(args, string_format);
	vsnprintf(buf, 1024, string_format, args);
	log->info(buf);
	va_end(args);
}

void LogInstance::warn(const char* string_format, ...) {
	char buf[1024];
	va_list args;
	va_start(args, string_format);
	vsnprintf(buf, 1024, string_format, args);
	log->warn(buf);
	va_end(args);
}

void LogInstance::error(const char* string_format, ...) {
	char buf[1024];
	va_list args;
	va_start(args, string_format);
	vsnprintf(buf, 1024, string_format, args);
	log->error(buf);
	va_end(args);
}
