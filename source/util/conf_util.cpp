/*
 * conf_util.cpp
 *
 *  Created on: 2013-8-24
 *      Author: Yan Shankai
 */

#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/util/conf_util.h"

ConfUtil* ConfUtil::m_instance = NULL;
ConfUtil::__Garbage__ ConfUtil::m_garbage;

ConfUtil::ConfUtil() {

}

ConfUtil::~ConfUtil() {
	for (map<const char*, ConfInstance*>::const_iterator iter =
			m_conf_instances.begin(); iter != m_conf_instances.end(); iter++) {
		if (iter->second != NULL) {
			delete iter->second;
		}
	}
}

ConfUtil* ConfUtil::get_instance() {
	if (m_instance != NULL) {
		return m_instance;
	} else {
		m_instance = new ConfUtil();
		return m_instance;
	}
}

void ConfUtil::destroy_instance() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}

ConfInstance* ConfUtil::get_conf_instance(const char* identifier) {
	for (map<const char*, ConfInstance*>::const_iterator iter =
			m_conf_instances.begin(); iter != m_conf_instances.end(); iter++) {
		if (strcmp(iter->first, identifier) == 0) {
			return iter->second;
		}
	}
	char conf_path[PATH_MAX];
	getcwd(conf_path, PATH_MAX); //获取当前目录
	strcat(conf_path, "/shared/");
	strcat(conf_path, identifier);
	strcat(conf_path, ".conf");
	FILE* fd = fopen(conf_path, "r");
	if (fd == NULL) {
		return NULL;
	}
	fclose(fd);
	ConfInstance* conf_ptr = new ConfInstance(identifier, conf_path);
	m_conf_instances.insert(
			pair<const char*, ConfInstance*>(identifier, conf_ptr));
	return conf_ptr;
}

ConfInstance::ConfInstance(const char* identifier, const char* conf_path) {
	m_identifier = identifier;
	FILE* fd = fopen(conf_path, "r");
	const unsigned int MAXLINE = 1024;
	char buf[MAXLINE];
	unsigned int line = 1;
	while (fgets(buf, MAXLINE, fd) != NULL) {
		bool is_comment = false; //判断是否是注释
		//忽略开始和结尾的空格
		unsigned int conf_buf_size;
		char* conf_buf = trim(buf, " \t\n", conf_buf_size);
		if (conf_buf == NULL) {
			continue; //整行都是定界符
		}
		memset(conf_buf + conf_buf_size, '\0', 1);
		is_comment = (*conf_buf == '#');
		if (!is_comment) {
			//分隔配置的名称和值
			char* p[3];
			int in = 0;
			while ((p[in] = strtok(conf_buf, "=")) != NULL) {
				in++;
				conf_buf = NULL;
			}
			//忽略名称结尾的空格
			unsigned int conf_key_len;
			p[0] = trim_right(p[0], " \t\n", conf_key_len);
			if (p[0] == NULL) {
				printf(
						"The key on the line %u of configure file %s is invalid!\n",
						line, conf_path);
			}
			memset(p[0] + conf_key_len, '\0', 1);
			//查看之前是否已经定义相同的配置
			map<char*, char*>::iterator configuration = m_configuration.find(
					p[0]);
			if (configuration != m_configuration.end()) {
				continue;
			} else {
				//插入新配置
				char* key = new char[strlen(p[0]) + 1];
				strcpy(key, p[0]);
				//忽略值开始的空格
				unsigned int conf_value_len;
				p[1] = trim_left(p[1], " \t\n", conf_value_len);
				char* value = new char[strlen(p[1]) + 1];
				strcpy(value, p[1]);
				m_configuration.insert(pair<char*, char*>(key, value));
			}
		}
		line++;
	}
	fclose(fd);
}

ConfInstance::~ConfInstance() {
	for (map<char*, char*>::iterator iter = m_configuration.begin();
			iter != m_configuration.end(); iter++) {
		if (iter->first != NULL) {
			delete[] iter->first;
		}
		if (iter->second != NULL) {
			delete[] iter->second;
		}
	}
}

const char* ConfInstance::get_configuration(const char* conf_name) {
	for (map<char*, char*>::const_iterator iter = m_configuration.begin();
			iter != m_configuration.end(); iter++) {
		if (strcmp(iter->first, conf_name) == 0) {
			return (const char*) iter->second;
		}
	}
	return NULL;
}
