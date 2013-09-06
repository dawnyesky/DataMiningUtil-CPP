/*
 * extractor_test.cpp
 *
 *  Created on: 2011-12-6
 *      Author: Yan Shankai
 */

#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/util/sysinfo_util.h"
#include "libyskdmu/index/hash_index.h"
#include "libyskdmu/index/open_hash_index.h"
#include "libyskdmu/association/extractor/trade_x_extractor.h"
#include "libyskdmu/association/extractor/doc_text_extractor.h"

static clock_t start, finish;
static float duration;

void test_trade_x_extractor() {
	printf("**********TradeXmlExtractor start testing**********\n");
	start = clock();
	bool print_index = false;
	bool print_record = true;
	unsigned int table_size = 1000;
	vector<RecordInfo> record_infos;
	vector<vector<Item> > items;
	vector<ItemDetail> item_details;
	OpenHashIndex* item_index = new OpenHashIndex(table_size);
	TradeXmlExtractor trade_x = TradeXmlExtractor(&record_infos, &items,
			&item_details, item_index);
//	trade_x.hi_extract_record((char*) "./shared/DataRecords/2011.11.11.xml");
	trade_x.read_data(false);

	if (print_index) {
		printf("Index:\n");
		IndexHead **hash_table = (IndexHead **) item_index->get_hash_table();
		const char *identifier = NULL;
		for (unsigned int i = 0; i < table_size; i++) {
			if (hash_table[i] != NULL) {
				identifier = item_details[ysk_atoi(hash_table[i]->key_info,
						strlen(hash_table[i]->key_info))].m_identifier;
				printf("slot: %u\thashcode: %u\tkey: %s------Record index: ", i,
						item_index->hashfunc(identifier, strlen(identifier)),
						identifier);
				IndexItem *p = hash_table[i]->inverted_index;
				while (p != NULL) {
					printf("%u, ", p->record_id);
					p = p->next;
				}
				printf("\n");
			}
		}
		printf("\n");
	}

	if (print_record) {
		printf("Record:\n");
		for (unsigned int i = 0; i < record_infos.size(); i++) {
			printf("ID: %u\tTid: %llu\tIterms: ", i, record_infos[i].m_tid);
			for (unsigned int j = 0; j < items[i].size(); j++) {
				printf("%s[%u], ",
						item_details[items[i][j].m_index].m_identifier,
						items[i][j].get_count());
			}
			printf("\n");
		}
	}

	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf(
			"TradeXmlExtractor testing duaration: %f(secs), memory occupation: %u(kb)\n",
			duration, SysInfoUtil::get_cur_proc_mem_usage());
	printf("**********TradeXmlExtractor finish testing**********\n\n");
}

void test_doc_text_extractor() {
	printf("**********DocTextExtractor start testing**********\n");
	start = clock();
	bool print_index = false;
	bool print_record = true;
	unsigned int table_size = 70000;
	vector<DocTextRecordInfo> record_infos;
	vector<vector<DocItem> > items;
	vector<DocItemDetail> item_details;
	OpenHashIndex item_index = OpenHashIndex(table_size);
	DocTextExtractor doc_text = DocTextExtractor(&record_infos, &items,
			&item_details, &item_index);

//	if (!ICTCLAS_Init(ICTCLAS_INIT_DIR)) {
//		printf("ICTCLAS Init Failed.\n");
//		return;
//	}
//	ICTCLAS_SetPOSmap(ICT_POS_MAP_SECOND);
//	doc_text.extract_records((char*) "./shared/GoalFiles/611.txt");
//	ICTCLAS_Exit();

	doc_text.read_data(true);

//	FILE *log_fp = freopen("./log/test_extractor.log", "w", stderr);
	if (print_index) {
		printf("Index:\n");
		IndexHead **hash_table = (IndexHead **) item_index.get_hash_table();
		const char *identifier = NULL;
		for (unsigned int i = 0; i < table_size; i++) {
			if (hash_table[i] != NULL) {
				identifier = item_details[ysk_atoi(hash_table[i]->key_info,
						strlen(hash_table[i]->key_info))].m_identifier;
				printf("slot: %u\thashcode: %u\tkey: %s------Record index: ", i,
						item_index.hashfunc(identifier, strlen(identifier)),
						identifier);
				IndexItem *p = hash_table[i]->inverted_index;
				while (p != NULL) {
					printf("%u, ", p->record_id);
					p = p->next;
				}
				printf("\n");
			}
		}
		printf("\n");
	}

	if (print_record) {
		printf("Record:\n");
		for (unsigned int i = 0; i < record_infos.size(); i++) {
			printf("ID: %u\tDocument name: %s\tIterms: ", i,
					record_infos[i].m_doc_name);
//			fprintf(stderr, "ID: %u\tDocument name: %s\tIterms: ", i, record_infos[i].m_doc_name);
			for (unsigned int j = 0; j < items[i].size(); j++) {
				printf("%s[%u], ",
						item_details[items[i][j].m_index].m_identifier,
						items[i][j].get_count());
//				fprintf(stderr, "%s[%u], ",
//										item_details[items[i][j].m_index].m_identifier,
//										items[i][j].get_count());
			}
			printf("\n");
//			fprintf(stderr, "\n");
		}
	}
//	fclose(log_fp);

	printf("Index size is: %f(MB)\n",
			item_index.size_of_index() / (1024.0 * 1024));
	finish = clock();
	duration = (float) (finish - start) / (CLOCKS_PER_SEC);
	printf(
			"DocTextExtractor testing duaration: %f(secs), memory occupation: %u(kb)\n",
			duration, SysInfoUtil::get_cur_proc_mem_usage());
	printf("**********DocTextExtractor finish testing**********\n\n");
}
