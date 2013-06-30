/*
 * trade_x_extractor.h
 *
 *  Created on: 2011-12-4
 *      Author: Yan Shankai
 */

#ifndef TRADE_X_EXTRACTOR_H_
#define TRADE_X_EXTRACTOR_H_

#include "libyskdmu/association/entity/item.h"
#include "libyskdmu/association/extractor/extractor_interface.h"

using namespace std;

static const char TRADE_INPUT_DIR[] = "./shared/DataRecords/";

class TradeXmlExtractor: public Extractor<Item, ItemDetail, RecordInfo> {
public:
	TradeXmlExtractor();
	TradeXmlExtractor(vector<RecordInfo>* record_infos,
			vector<vector<Item> >* items, vector<ItemDetail>* item_details,
			OpenHashIndex* item_index);
	virtual ~TradeXmlExtractor();
	void read_data(bool with_hi);
	bool extract_record(void* data_addr);
	bool hi_extract_record(void* data_addr);

private:
	pair<char**, unsigned int> parse_items(const char* data_str, size_t length);
	void free_char_array(pair<char**, unsigned int>& char_array);
};

#endif /* TRADE_X_EXTRACTOR_H_ */
