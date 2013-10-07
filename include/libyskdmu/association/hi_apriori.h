/*
 * hi_apriori.h
 *
 *  Created on: 2011-12-22
 *      Author: Yan Shankai
 */

#ifndef HI_APRIORI_H_
#define HI_APRIORI_H_

#include "libyskdmu/index/hash_index.h"
#include "libyskdmu/index/open_hash_index.h"
#include "libyskdmu/association/entity/itemset.h"
#include "libyskdmu/association/apriori.h"

using namespace std;

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
class HiApriori: public Apriori<ItemType, ItemDetail, RecordInfoType> {
public:
	HiApriori();
	HiApriori(unsigned int hi_table_size);
	virtual ~HiApriori();
	virtual bool init(unsigned int max_itemset_size, double minsup,
			double minconf);

	/*
	 * description: HI-Apriori 频繁项集生成算法
	 *      return: 生成频繁项集是否成功
	 */
	bool hi_apriori();
	void set_extractor(
			Extractor<ItemType, ItemDetail, RecordInfoType>* extractor);
	virtual unsigned int get_support_count(const vector<unsigned int>& itemset);

protected:
	/*
	 * description: 频繁项集生成函数 	F(k-1)xF(1)Method
	 *  parameters: frq_itemset：	频繁项集容器
	 *  			  prv_frq：		(k-1)项频繁项集
	 *  			  frq_1：			1项频繁项集
	 *      return: 生成频繁项集是否成功
	 */
	bool hi_frq_gen(KItemsets& frq_itemset, KItemsets& prv_frq1,
			KItemsets& prv_frq2);
	/*
	 * description: HI-Apriori算法频繁项集过滤器
	 *  parameters: k_itemset：	需要检查的k项频繁项集
	 *      return: 是否是频繁项集
	 */
	bool hi_filter(vector<unsigned int>* k_itemset, unsigned int* support);

public:
	HashIndex* m_item_index; //以m_item_details的索引作为KeyInfo
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
HiApriori<ItemType, ItemDetail, RecordInfoType>::HiApriori() {
	m_item_index = new OpenHashIndex();
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
HiApriori<ItemType, ItemDetail, RecordInfoType>::HiApriori(
		unsigned int hi_table_size) {
	m_item_index = new OpenHashIndex(hi_table_size);
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
HiApriori<ItemType, ItemDetail, RecordInfoType>::~HiApriori() {
	if (m_item_index != NULL) {
		delete m_item_index;
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool HiApriori<ItemType, ItemDetail, RecordInfoType>::init(
		unsigned int max_itemset_size, double minsup, double minconf) {
	this->m_max_itemset_size = max_itemset_size;
	this->m_minsup = minsup;
	this->m_minconf = minconf;
	this->log = LogUtil::get_instance()->get_log_instance("hiApriori");
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool HiApriori<ItemType, ItemDetail, RecordInfoType>::hi_apriori() {
	// 读取数据集
	this->m_extractor->read_data(true);

	if (this->m_record_infos.size() == 0 || this->m_minsup <= 0
			|| this->m_minconf <= 0) {
		return false;
	}

	this->m_minsup_count = double2int(
			this->m_record_infos.size() * this->m_minsup);
	if (0 == this->m_minsup_count)
		this->m_minsup_count = 1;
	vector<unsigned int> itemset;
	KItemsets *frq_itemsets;

	/* F1 generation */
	frq_itemsets = new KItemsets(1);
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		unsigned int result;
		result = m_item_index->get_mark_record_num(
				this->m_item_details[i].m_identifier,
				strlen(this->m_item_details[i].m_identifier));
		if (result >= this->m_minsup_count) {
			itemset.clear();
			itemset.push_back(i);
			frq_itemsets->push(itemset, result);
			this->logItemset("Frequent", 1, itemset, result);
		}
	}
	if (frq_itemsets->get_itemsets().size() == 0) { //frequent 1-itemsets is not found
		return false;
	}
	this->m_frequent_itemsets->push_back(*frq_itemsets);
	delete frq_itemsets;

	/* F2~n generation */
	for (unsigned int i = 0;
			this->m_frequent_itemsets->size() == i + 1
					&& i + 1 < this->m_max_itemset_size; i++) {
		frq_itemsets = new KItemsets(i + 2,
				1.5 * combine(this->m_item_details.size(), i + 2));
		if (!hi_frq_gen(*frq_itemsets, this->m_frequent_itemsets->at(i),
				this->m_frequent_itemsets->at(0))) {
			return false;
		}
		if (frq_itemsets->get_itemsets().size() > 0) {
			this->m_frequent_itemsets->push_back(*frq_itemsets);
		}
		delete frq_itemsets;
	}
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool HiApriori<ItemType, ItemDetail, RecordInfoType>::hi_frq_gen(
		KItemsets& frq_itemset, KItemsets& prv_frq1, KItemsets& prv_frq2) {
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets1 =
			prv_frq1.get_itemsets();
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets2 =
			prv_frq2.get_itemsets();
	vector<unsigned int>* k_itemset = NULL;

	/* 潜在频繁项集生成 */
	for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq1_iter =
			prv_frq_itemsets1.begin(); prv_frq1_iter != prv_frq_itemsets1.end();
			prv_frq1_iter++) {
		for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq2_iter =
				prv_frq_itemsets2.begin();
				prv_frq2_iter != prv_frq_itemsets2.end(); prv_frq2_iter++) {

			/************************** 项目集连接与过滤 **************************/
			//求并集
			if (prv_frq1_iter->first.size() == 1
					|| prv_frq2_iter->first.size() == 1) { //F(k-1)×F(1)
				k_itemset = KItemsets::union_set(prv_frq1_iter->first,
						prv_frq2_iter->first);
			} else if (prv_frq1_iter->first.size()
					== prv_frq2_iter->first.size()) { //F(k-1)×F(k-1)
				k_itemset = KItemsets::union_eq_set(prv_frq1_iter->first,
						prv_frq2_iter->first);
			}

			//过滤并保存潜在频繁项集
			unsigned int support;
			if (k_itemset != NULL
					&& k_itemset->size() == frq_itemset.get_term_num()
					&& hi_filter(k_itemset, &support)) {
				frq_itemset.push(*k_itemset, support);
				this->logItemset("Frequent", k_itemset->size(), *k_itemset,
						support);
			}
			delete k_itemset;
		}
	}

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool HiApriori<ItemType, ItemDetail, RecordInfoType>::hi_filter(
		vector<unsigned int>* k_itemset, unsigned int* support) {
	char **keys = new char*[k_itemset->size()];
	if (0 == this->m_minsup_count)
		this->m_minsup_count = 1;
	unsigned int *result;
	for (unsigned int j = 0; j < k_itemset->size(); j++) {
		keys[j] = this->m_item_details[k_itemset->at(j)].m_identifier;
	}
	result = m_item_index->get_intersect_records((const char **) keys,
			k_itemset->size());
	*support = result[0];
	delete[] keys;
	delete[] result;

	return *support >= this->m_minsup_count;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void HiApriori<ItemType, ItemDetail, RecordInfoType>::set_extractor(
		Extractor<ItemType, ItemDetail, RecordInfoType>* extractor) {
	this->m_extractor = extractor;
	this->m_extractor->set_record_infos(&this->m_record_infos);
//	this->m_extractor->set_items(&this->m_items);
	this->m_extractor->set_item_details(&this->m_item_details);
	this->m_extractor->set_item_index(this->m_item_index);
	this->m_extractor->m_assoc = this;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
unsigned int HiApriori<ItemType, ItemDetail, RecordInfoType>::get_support_count(
		const vector<unsigned int>& itemset) {
	char **keys = new char*[itemset.size()];
	unsigned int *result;
	for (unsigned int i = 0; i < itemset.size(); i++) {
		keys[i] = this->m_item_details[itemset[i]].m_identifier;
	}
	result = m_item_index->get_intersect_records((const char **) keys,
			itemset.size());
	unsigned int support_count = result[0];
	delete[] keys;
	delete[] result;
	return support_count;

}

#endif /* HI_APRIORI_H_ */
