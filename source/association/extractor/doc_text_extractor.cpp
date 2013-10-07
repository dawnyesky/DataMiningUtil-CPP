/*
 * doc_text_extractor.cpp
 *
 *  Created on: 2011-12-8
 *      Author: Yan Shankai
 */

#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/util/charset_util.h"
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/util/conf_util.h"
#include "libyskdmu/association/extractor/doc_text_extractor.h"

DocTextExtractor::DocTextExtractor() {

}

DocTextExtractor::DocTextExtractor(vector<DocTextRecordInfo>* record_infos,
		vector<vector<DocItem> >* items, vector<DocItemDetail>* item_details,
		HashIndex* item_index) {
	m_record_infos = record_infos;
	m_items = items;
	m_item_details = item_details;
	m_item_index = item_index;
	this->init();
}

DocTextExtractor::~DocTextExtractor() {

}

//做成守护进程，遇到新文件就调用extract_records()
void DocTextExtractor::read_data(bool with_hi) {
	ConfInstance* conf_instance = ConfUtil::get_instance()->get_conf_instance(
			"global");
	const char* conf_root_dir = conf_instance->get_configuration("ROOT_DIR");
	const char* ictclas_init_short_dir = conf_instance->get_configuration(
			"ICTCLAS_INIT_DIR");
	char ictclas_init_dir[strlen(conf_root_dir) + strlen(ictclas_init_short_dir)
			+ 1];
	strcpy(ictclas_init_dir, conf_root_dir);
	strcat(ictclas_init_dir, ictclas_init_short_dir);
	if (!ICTCLAS_Init(ictclas_init_dir)) {
		log->error("ICTCLAS Init Failed.\n");
		return;
	}
	ICTCLAS_SetPOSmap(ICT_POS_MAP_SECOND);

	const char* input_short_dir = conf_instance->get_configuration("INPUT_DIR");
	char input_dir[strlen(conf_root_dir) + strlen(input_short_dir) + 2];
	strcpy(input_dir, conf_root_dir);
	strcat(input_dir, input_short_dir);
	strcat(input_dir, "/");

	dirent *entry = NULL;
	DIR *pDir = opendir(input_dir);
	char fpath[strlen(input_dir) + 256];
	m_index.clear();
	m_counter.clear();
	unsigned long long int tid = 0;
	while (NULL != (entry = readdir(pDir))) {
		if (entry->d_type == 8) {
			//普通文件
			strcpy(fpath, input_dir);
			bool succeed = false;
			if (with_hi) {
				succeed = hi_extract_record(strcat(fpath, entry->d_name));
			} else {
				succeed = extract_record(strcat(fpath, entry->d_name));
			}
			if (succeed && m_record_infos != NULL) {
				m_record_infos->at(tid).m_tid = tid++;
			}
		} else {
			//目录
		}
	}
	closedir(pDir);

	ICTCLAS_Exit();
}

bool DocTextExtractor::extract_record(void* data_addr) {
	/************************ 读取整个txt文件到内存 ************************/
	char* file_path = (char*) data_addr;
	int file_descriptor;
	long file_size;
	file_descriptor = open(file_path, O_RDONLY); //以只读方式打开源文件
	if (file_descriptor == -1) {
		if (log->isWarnEnabled()) {
			log->warn("打开文件%s出错！\n", file_path);
		}
		return false;
	}

	//抽取record_info
	if (m_record_infos != NULL) {
		DocTextRecordInfo record_info = DocTextRecordInfo(file_path);
		m_record_infos->push_back(record_info);
	}

	//抽取data
	file_size = lseek(file_descriptor, 0, SEEK_END); //计算文件大小
	lseek(file_descriptor, 0, SEEK_SET); //把文件指针重新移到开始位置
	char text[file_size]; //最后一个字节是EOF
	read(file_descriptor, text, file_size - 1);
	memset(text + file_size - 1, 0, 1);

	/************************** 分词，获取关键字 **************************/
	int result_count = 0;
	map<string, unsigned int> index;
	static const size_t buf_size = 100; //缓冲区大小
	char word[buf_size]; //分词结果缓冲区
	char temp[buf_size];
	vector<DocItem> v_items;
	LPICTCLAS_RESULT parsed_words = ICTCLAS_ParagraphProcessA(text,
			strlen(text), result_count, CODETYPE, true);
	for (int i = 0; i < result_count; i++) {
		//词性过滤，只要名词、动词、形容词，构造关键词序列
		bool filter_bool = (parsed_words[i].szPOS[0] == 'n' //名词
		&& (parsed_words[i].szPOS[1] == 'r' //人名
		|| parsed_words[i].szPOS[1] == 's')) //地名
		|| parsed_words[i].szPOS[0] == 'v' //动词
		|| parsed_words[i].szPOS[0] == 'a'; //形容词
		if (filter_bool && parsed_words[i].iWordID > 0) {
			//对过滤后的关键字进行编码转换
			if (CODETYPE == CODE_TYPE_GB) {
				memcpy(temp, text + parsed_words[i].iStartPos,
						parsed_words[i].iLength);
				memset(temp + parsed_words[i].iLength, 0, 1);
				if (-1
						== convert_charset("utf-8", "gb2312", word, buf_size,
								temp, strlen(temp))) {
					fprintf(stderr, "word %s charset convert failed!\n", word);
				}
			} else {
				memcpy(word, text + parsed_words[i].iStartPos,
						parsed_words[i].iLength);
				memset(word + parsed_words[i].iLength, 0, 1);
			}

			unsigned int key_info = 0;
			//抽取item_detail
			string word_str = string(word);
			map<string, unsigned int>::const_iterator key_info_iter =
					m_index.find(word_str);
			if (key_info_iter == m_index.end()) {
				m_item_details->push_back(
						DocItemDetail(word, parsed_words[i].szPOS));
				key_info = m_item_details->size() - 1;
				index.insert(
						std::map<string, unsigned int>::value_type(word_str,
								key_info));
				m_index.insert(
						std::map<string, unsigned int>::value_type(word_str,
								key_info));
				m_counter.insert(
						std::map<string, unsigned int>::value_type(word_str,
								1));
			} else {
				key_info = key_info_iter->second;
				if (index.find(word_str) == index.end()) {
					index.insert(
							std::map<string, unsigned int>::value_type(word_str,
									key_info));
					m_counter.at(word_str)++;}
				}

				//抽取item
			DocItem item = DocItem(key_info, parsed_words[i].iStartPos,
					parsed_words[i].iLength);
			pair<unsigned int, bool> bs_result = b_search<DocItem>(v_items,
					item);
			if (bs_result.second) {
				v_items[bs_result.first].increase();
				v_items[bs_result.first].update(parsed_words[i].iStartPos);
			} else {
				v_items.insert(v_items.begin() + bs_result.first, item);
			}
		}
	}
	if (v_items.size() > 0) {
		//回调函数
		if (m_ihandler != NULL) {
			vector<unsigned int> record;
			for (unsigned int j = 0; j < v_items.size(); j++) {
				record.push_back(v_items[j].m_index);
			}
			(this->m_ihandler)(this->m_assoc, record);
		}
		if (m_items != NULL) {
			m_items->push_back(v_items);
		}
	}

	ICTCLAS_ResultFree(parsed_words);
	close(file_descriptor);
	return true;
}

bool DocTextExtractor::hi_extract_record(void* data_addr) {
	/************************ 读取整个txt文件到内存 ************************/
	char* file_path = (char*) data_addr;
	int file_descriptor;
	long file_size;
	file_descriptor = open(file_path, O_RDONLY); //以只读方式打开源文件
	if (file_descriptor == -1) {
		if (log->isWarnEnabled()) {
			log->warn("打开文件%s出错！\n", file_path);
		}
		return false;
	}

	//抽取record_info
	DocTextRecordInfo record_info = DocTextRecordInfo(file_path);
	m_record_infos->push_back(record_info);

	//抽取data
	file_size = lseek(file_descriptor, 0, SEEK_END); //计算文件大小
	lseek(file_descriptor, 0, SEEK_SET); //把文件指针重新移到开始位置
	char text[file_size]; //最后一个字节是EOF
	read(file_descriptor, text, file_size - 1);
	memset(text + file_size - 1, 0, 1);

	/************************** 分词，获取关键字 **************************/
	int result_count = 0;
	static const size_t buf_size = 100; //缓冲区大小
	char word[buf_size]; //分词结果缓冲区
	char temp[buf_size];
	vector<DocItem> v_items;
	LPICTCLAS_RESULT parsed_words = ICTCLAS_ParagraphProcessA(text,
			strlen(text), result_count, CODETYPE, true);
	for (int i = 0; i < result_count; i++) {
		//词性过滤，只要名词、动词、形容词，构造关键词序列
		bool filter_bool = (parsed_words[i].szPOS[0] == 'n' //名词
		&& (parsed_words[i].szPOS[1] == 'r' //人名
		|| parsed_words[i].szPOS[1] == 's')) //地名
		|| parsed_words[i].szPOS[0] == 'v' //动词
		|| parsed_words[i].szPOS[0] == 'a'; //形容词
		if (filter_bool && parsed_words[i].iWordID > 0) {
			//对过滤后的关键字进行编码转换
			if (CODETYPE == CODE_TYPE_GB) {
				memcpy(temp, text + parsed_words[i].iStartPos,
						parsed_words[i].iLength);
				memset(temp + parsed_words[i].iLength, 0, 1);
				if (-1
						== convert_charset("utf-8", "gb2312", word, buf_size,
								temp, strlen(temp))) {
					fprintf(stderr, "word %s charset convert failed!\n", word);
				}
			} else {
				memcpy(word, text + parsed_words[i].iStartPos,
						parsed_words[i].iLength);
				memset(word + parsed_words[i].iLength, 0, 1);
			}

			char* key_info = NULL; //此处key_info指向了NULL，传入函数时要用&key_info，否则返回仍然指向NULL
			bool have_index = true;
			size_t length = strlen(word);
			//抽取item_detail
			if (!m_item_index->get_key_info(&key_info, word, length)) {
				m_item_details->push_back(
						DocItemDetail(word, parsed_words[i].szPOS));
				key_info = itoa(m_item_details->size() - 1);
				have_index = false;
			}

			//抽取item
			DocItem item = DocItem(ysk_atoi(key_info, strlen(key_info)),
					parsed_words[i].iStartPos, parsed_words[i].iLength);
			pair<unsigned int, bool> bs_result = b_search<DocItem>(v_items,
					item);
			if (bs_result.second) {
				v_items[bs_result.first].increase();
				v_items[bs_result.first].update(parsed_words[i].iStartPos);
			} else {
				v_items.insert(v_items.begin() + bs_result.first, item);
				have_index = false;
			}

			//添加索引
			if (!have_index) {
				m_item_index->insert(word, length, key_info,
						m_record_infos->size() - 1);
			}

			if (key_info != NULL) {
				delete[] key_info;
			}
		}
	}
	if (m_items != NULL) {
		m_items->push_back(v_items);
	}

	ICTCLAS_ResultFree(parsed_words);
	close(file_descriptor);
	return true;
}
