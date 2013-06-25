/*
 * trade_text_extractor.h
 *
 *  Created on: 2012-11-14
 *      Author: "Yan Shankai"
 */

#ifndef TRADE_TEXT_EXTRACTOR_H_
#define TRADE_TEXT_EXTRACTOR_H_

#include "libyskdmu/association/entity/item.h"
#include "libyskdmu/association/extractor/extractor_interface.h"

using namespace std;

class TradeTextExtractor: public Extractor<Item, ItemDetail, RecordInfo> {
public:
	TradeTextExtractor();
	TradeTextExtractor(vector<RecordInfo>* record_infos,
			vector<vector<Item> >* items, vector<ItemDetail>* item_details,
			HashIndex* item_index);
	virtual ~TradeTextExtractor();
	void read_data(bool with_hi);
	bool extract_record(void* data_addr);
	bool hi_extract_record(void* data_addr);

	void set_data(map<unsigned int, map<unsigned int, double>*>* data);

private:
	map<unsigned int, map<unsigned int, double>*>* m_data;
};

#endif /* TRADE_TEXT_EXTRACTOR_H_ */
