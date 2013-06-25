/*
 * extractor_interface.h
 *
 *  Created on: 2012-3-23
 *      Author: caspar
 */

#ifndef EXTRACTOR_INTERFACE_H_
#define EXTRACTOR_INTERFACE_H_

#include <vector>
#include "libyskdmu/index/hash_index.h"
#include <string>

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
class Apriori;

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
class DHP;

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
class FpGrowth;

struct FpTreeNode;

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
class Extractor {
public:
	// 记录处理器函数指针类型模板
	typedef void (*AprioriItemsHandler)(
			Apriori<ItemType, ItemDetailType, RecordInfoType>* apriori,
			vector<unsigned int>& record);
	typedef void (*DHPItemsHandler)(
			DHP<ItemType, ItemDetailType, RecordInfoType>* dhp,
			vector<unsigned int>& record);
	typedef void (*FpGrowthItemsHandler)(
			FpGrowth<ItemType, ItemDetailType, RecordInfoType>* fp_growth,
			vector<unsigned int>& record);

	Extractor();
	virtual ~Extractor();
	void set_record_infos(vector<RecordInfoType>* record_infos);
	void set_items(vector<vector<ItemType> >* items);
	void set_item_details(vector<ItemDetailType>* item_details);
	void set_items_handler(AprioriItemsHandler items_handler);
	void set_items_handler(DHPItemsHandler items_handler);
	void set_items_handler(FpGrowthItemsHandler items_handler);
	void set_item_index(HashIndex* item_index);
	map<string, unsigned int>& get_counter();
	virtual void init();
	/*
	 * description: 对k-item进行计数
	 *  parameters: item元组的索引列表
	 *      return: k-item内出现重复或者维数不符合计数器初始化时设定的维数时返回false，其余返回true
	 */
	virtual void read_data(bool with_hi) = 0;
	virtual bool extract_record(void* data_addr) = 0;
	virtual bool hi_extract_record(void* data_addr) = 0;

public:
	Apriori<ItemType, ItemDetailType, RecordInfoType>* m_apriori;
	FpGrowth<ItemType, ItemDetailType, RecordInfoType>* m_fp_growth;
	DHP<ItemType, ItemDetailType, RecordInfoType>* m_dhp;
	map<string, unsigned int> m_counter;
	map<string, unsigned int> m_index;

protected:
	vector<RecordInfoType>* m_record_infos;
	vector<vector<ItemType> >* m_items;
	vector<ItemDetailType>* m_item_details;
	AprioriItemsHandler m_apriori_ihandler;
	DHPItemsHandler m_dhp_ihandler;
	FpGrowthItemsHandler m_fp_growth_ihandler;
	HashIndex* m_item_index;
	LogInstance* log; //日志指针
};

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
Extractor<ItemType, ItemDetailType, RecordInfoType>::Extractor() {
	m_record_infos = NULL;
	m_items = NULL;
	m_item_details = NULL;
	m_apriori_ihandler = NULL;
	m_dhp_ihandler = NULL;
	m_fp_growth_ihandler = NULL;
	m_item_index = NULL;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
Extractor<ItemType, ItemDetailType, RecordInfoType>::~Extractor() {
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::init() {
	log = LogUtil::get_instance()->get_log_instance("extractor");
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::set_record_infos(
		vector<RecordInfoType>* record_infos) {
	m_record_infos = record_infos;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::set_items(
		vector<vector<ItemType> >* items) {
	m_items = items;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::set_item_details(
		vector<ItemDetailType>* item_details) {
	m_item_details = item_details;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::set_items_handler(
		AprioriItemsHandler items_handler) {
	m_apriori_ihandler = items_handler;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::set_items_handler(
		DHPItemsHandler items_handler) {
	m_dhp_ihandler = items_handler;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::set_items_handler(
		FpGrowthItemsHandler items_handler) {
	m_fp_growth_ihandler = items_handler;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
void Extractor<ItemType, ItemDetailType, RecordInfoType>::set_item_index(
		HashIndex* item_index) {
	m_item_index = item_index;
}

template<typename ItemType, typename ItemDetailType, typename RecordInfoType>
map<string, unsigned int>& Extractor<ItemType, ItemDetailType, RecordInfoType>::get_counter() {
	return m_counter;
}

#endif /* EXTRACTOR_INTERFACE_H_ */
