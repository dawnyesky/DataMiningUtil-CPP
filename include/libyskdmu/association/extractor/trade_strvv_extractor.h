/*
 * trade_strvv_extractor.h
 *
 *  Created on: 2012-8-14
 *      Author: "Yan Shankai"
 */

#ifndef TRADE_STRVV_EXTRACTOR_H_
#define TRADE_STRVV_EXTRACTOR_H_

#include "libyskdmu/association/entity/item.h"
#include "libyskdmu/association/extractor/extractor_interface.h"

using namespace std;

class TradeStrvvExtractor: public Extractor<Item, ItemDetail, RecordInfo> {
public:
	TradeStrvvExtractor();
	TradeStrvvExtractor(vector<RecordInfo>* record_infos,
			vector<vector<Item> >* items, vector<ItemDetail>* item_details,
			OpenHashIndex* item_index);
	virtual ~TradeStrvvExtractor();
	void read_data(bool with_hi);
	bool extract_record(void* data_addr);
	bool hi_extract_record(void* data_addr);

	unsigned int push_record(RecordInfo& record, vector<Item>& items);
	unsigned int push_detail(ItemDetail& detail);
	void clear_data_source();


private:
	pair<vector<RecordInfo>, vector<vector<Item> > > m_data;
	vector<ItemDetail> m_data_details;
};

#endif /* TRADE_STRVV_EXTRACTOR_H_ */
