/*
 * mpi_doc_text_extractor.cpp
 *
 *  Created on: 2013-8-5
 *      Author: Yan Shankai
 */

#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/util/charset_util.h"
#include "libyskdmu/util/search_util.h"
#include "libyskdmu/util/conf_util.h"
#include "libyskdmu/association/extractor/mpi_doc_text_extractor.h"

MPIDocTextExtractor::MPIDocTextExtractor(MPI_Comm comm, int root_pid) {
	m_comm = comm;
	m_root_pid = root_pid;
	m_record_offset = 0;
	m_item_detail_offset = 0;
	this->init();
}

MPIDocTextExtractor::MPIDocTextExtractor(
		vector<DocTextRecordInfo>* record_infos,
		vector<vector<DocItem> >* items, vector<DocItemDetail>* item_details,
		HashIndex* item_index, MPI_Comm comm, int root_pid) {
	m_record_infos = record_infos;
	m_items = items;
	m_item_details = item_details;
	m_item_index = item_index;
	m_comm = comm;
	m_root_pid = root_pid;
	m_record_offset = 0;
	m_item_detail_offset = 0;
	this->init();
}

MPIDocTextExtractor::~MPIDocTextExtractor() {

}

void MPIDocTextExtractor::read_data(bool with_hi) {
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

	char fpath[strlen(input_dir) + 256];
	strcpy(fpath, input_dir);

	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);

	pair<void*, int*> scafile_send_msg_pkg;
	pair<void*, int> scafile_recv_msg_pkg;
	int displs[numprocs];

	if (pid == m_root_pid) {
		dirent *entry = NULL;
		DIR *pDir = opendir(input_dir);
		m_index.clear();
		m_counter.clear();

		//读取目录下的所有文件
		vector<char*> files;
		while (NULL != (entry = readdir(pDir))) {
			if (entry->d_type == 8) {
				//普通文件
				unsigned int file_name_len = strlen(entry->d_name) + 1;
				char* file_name = new char[file_name_len];
				file_name[file_name_len - 1] = '\0';
				strcpy(file_name, entry->d_name);
				files.push_back(file_name);
			} else {
				//目录
			}
		}

		//准备Scatter数据
		assert(files.size() >= numprocs);
		unsigned int file_per_proc = files.size() / numprocs;
		vector < vector<char*> > v_files;
		vector<char*>::iterator iter = files.begin();
		for (unsigned int i = 0; i < numprocs - 1; i++) {
			vector<char*> files_per_proc = vector<char*>(iter,
					iter + file_per_proc);
			v_files.push_back(files_per_proc);
			iter += file_per_proc;
		}
		vector<char*> files_per_proc = vector<char*>(iter, files.end());
		v_files.push_back(files_per_proc);

		//打包Scatter消息
//		printf("start pack scafile msg\n");
		scafile_send_msg_pkg = pack_scafile_msg(v_files);
//		printf("end pack scafile msg\n");
		displs[0] = 0;
		for (unsigned int i = 1; i < numprocs; i++) {
			displs[i] = displs[i - 1] + scafile_send_msg_pkg.second[i - 1];
		}

		closedir(pDir);
		for (unsigned int i = 0; i < files.size(); i++) {
			delete[] files[i];
		}
	}

	//准备Scatter接收的数据缓冲区
	scafile_recv_msg_pkg.first = malloc(SCAFILE_BUF_SIZE);
	scafile_recv_msg_pkg.second = SCAFILE_BUF_SIZE;

	//发散数据
	MPI_Scatterv(scafile_send_msg_pkg.first, scafile_send_msg_pkg.second,
			displs, MPI_PACKED, scafile_recv_msg_pkg.first,
			scafile_recv_msg_pkg.second, MPI_PACKED, m_root_pid, m_comm);

	//解包Scatter消息
	pair<vector<char*>*, int> scafile_recv_msg = unpack_scafile_msg(
			scafile_recv_msg_pkg);

	//处理发散数据，读取本进程负责的资源
	m_record_offset = scafile_recv_msg.second;
	for (unsigned int i = 0; i < scafile_recv_msg.first->size(); i++) {
		strcpy(fpath, input_dir);
		if (hi_extract_record(strcat(fpath, scafile_recv_msg.first->at(i)))) {
			m_record_infos->at(m_record_infos->size() - 1).m_tid = i;
		}
	}

	if (pid == m_root_pid) {
		delete[] scafile_send_msg_pkg.second;
		free(scafile_send_msg_pkg.first);
	}
	free(scafile_recv_msg_pkg.first);

	for (unsigned int i = 0; i < scafile_recv_msg.first->size(); i++) {
		delete[] scafile_recv_msg.first->at(i);
	}
	delete scafile_recv_msg.first;

	ICTCLAS_Exit();
}

bool MPIDocTextExtractor::hi_extract_record(void* data_addr) {
	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);
	char* pid_str = itoa(pid, 10);

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
//	printf("Process %i is reading:%s\n", pid, file_path);
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

			char* key_info = NULL;
			int item_detail_index = -1;
			bool have_index = true;
			size_t length = strlen(word);
			//抽取item_detail
			if (!m_item_index->get_key_info(&key_info, word, length)) {
				m_item_details->push_back(
						DocItemDetail(word, parsed_words[i].szPOS));
				item_detail_index = m_item_details->size() - 1;
				key_info = itoa(item_detail_index);
				have_index = false;
			} else {
				char* p[3];
				char* buf = key_info;
				int in = 0;
				while ((p[in] = strtok(buf, "#")) != NULL) {
					in++;
					buf = NULL;
				}
				item_detail_index = ysk_atoi(p[1], strlen(p[1]));
				delete[] key_info;
				key_info = itoa(item_detail_index);
			}

			//抽取item
			DocItem item = DocItem(item_detail_index, parsed_words[i].iStartPos,
					parsed_words[i].iLength);
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
				char* global_key_info = new char[strlen(pid_str)
						+ strlen(key_info) + 2];
				strcpy(global_key_info, pid_str);
				strcat(global_key_info, "#");
				strcat(global_key_info, key_info);
//				printf("Global key_info:%s\n", global_key_info);
				m_item_index->insert(word, length, global_key_info,
						m_record_offset + m_record_infos->size() - 1);
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
	delete[] pid_str;
	return true;
}

pair<void*, int*> MPIDocTextExtractor::pack_scafile_msg(
		vector<vector<char*> >& v_files) {
	pair<void*, int*> result;

	//计算缓冲区大小
	result.second = new int[v_files.size()];
	int pack_size, total_size = 0;
	for (unsigned int i = 0; i < v_files.size(); i++) {
		int buf_size = 0;
		MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &pack_size); //资源起始ID和ID资源个数
		buf_size += 2 * pack_size;
		for (unsigned int j = 0; j < v_files[i].size(); j++) {
			MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &pack_size); //资源标识长度
			buf_size += pack_size;
			MPI_Pack_size(strlen(v_files[i][j]) + 1, MPI_CHAR, m_comm,
					&pack_size); //资源标识
			buf_size += pack_size;
		}
		result.second[i] = buf_size;
		total_size += buf_size;
	}

	//分配缓冲区空间
	result.first = malloc(total_size);
	//开始打包
	unsigned int id_offset = 0;
	int position = 0;
	for (unsigned int i = 0; i < v_files.size(); i++) {
		MPI_Pack(&id_offset, 1, MPI_UNSIGNED, result.first, total_size,
				&position, m_comm);
		unsigned int file_num = v_files[i].size();
		MPI_Pack(&file_num, 1, MPI_UNSIGNED, result.first, total_size,
				&position, m_comm);
		for (unsigned int j = 0; j < v_files[i].size(); j++) {
			unsigned int file_id_len = strlen(v_files[i][j]) + 1;
			MPI_Pack(&file_id_len, 1, MPI_UNSIGNED, result.first, total_size,
					&position, m_comm);
			MPI_Pack(v_files[i][j], file_id_len, MPI_CHAR, result.first,
					total_size, &position, m_comm);
		}
		id_offset += v_files[i].size();
	}

	return result;
}

pair<vector<char*>*, int> MPIDocTextExtractor::unpack_scafile_msg(
		pair<void*, int> msg_pkg) {
	pair<vector<char*>*, int> result;
	result.first = new vector<char*>();
	int position = 0;
	unsigned int file_num;
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &result.second, 1,
			MPI_UNSIGNED, m_comm);
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &file_num, 1,
			MPI_UNSIGNED, m_comm);
	for (unsigned int i = 0; i < file_num; i++) {
		unsigned int file_id_len;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &file_id_len, 1,
				MPI_UNSIGNED, m_comm);
		char* file = new char[file_id_len];
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, file, file_id_len,
				MPI_CHAR, m_comm);
		result.first->push_back(file);
	}
	return result;
}
