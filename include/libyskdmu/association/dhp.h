/*
 * dhp.h
 *
 *  Created on: 2012-5-29
 *      Author: "Yan Shankai"
 */

#ifndef DHP_H_
#define DHP_H_

#include "libyskdmu/association/apriori.h"

class DHPCounter: public HashTableCounter {
public:
	DHPCounter(unsigned int size, unsigned int dimension) :
			HashTableCounter(size, dimension) {
	}
	unsigned int collision_handler(const unsigned int k_item[],
			unsigned int collision_key) const {
		return collision_key;
	}
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
class DHP: public Apriori<ItemType, ItemDetail, RecordInfoType> {
public:
	DHP();
	virtual ~DHP();
	virtual bool init(unsigned int max_itemset_size, double minsup,
			double minconf);

	/*
	 * description: DHP 频繁项集生成算法
	 *      return: 生成频繁项集是否成功
	 */
	bool dhp();
	/*
	 * description: 候选集生成算法 F(k-1)xF(1)Method
	 *  parameters: candidate：	候选集容器
	 *  			  prv_frq：	(k-1)项频繁项集
	 *  			  frq_1：		1项频繁项集
	 *      return: 生成候选集是否成功
	 */
	bool candidate_gen(KItemsets& candidate, KItemsets& prv_frq,
			KItemsets& frq_1);
	/*
	 * description: DHP算法潜在频繁项集过滤器
	 *  parameters: k_itemset：	需要检查的k次项集
	 *      return: 是否是潜在频繁项集
	 */
	bool dhp_filter(vector<unsigned int>* k_itemset);
	void set_extractor(
			Extractor<ItemType, ItemDetail, RecordInfoType>* extractor);
	void bind_call_back();
	void create_counter();
	void destroy_counter();

public:
	vector<DHPCounter*> m_dhp_counter;

};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void dhp_call_back(
		AssocBase<ItemType, ItemDetail, RecordInfoType>* assoc_instance,
		vector<unsigned int>& record) {
	DHP<ItemType, ItemDetail, RecordInfoType>* dhp = (DHP<ItemType, ItemDetail,
			RecordInfoType>*) assoc_instance;
	if (NULL == dhp->m_current_itemsets)
		return;
	else {
		if (dhp->m_current_itemsets->get_term_num() > 1) {
			// 对当前项集进行统计
			const map<vector<unsigned int>, unsigned int>& kitemsets =
					dhp->m_current_itemsets->get_itemsets();
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					kitemsets.begin(); iter != kitemsets.end(); iter++) {
				if (is_subset<unsigned int>(record, iter->first)) {
					dhp->m_itemsets_counter[dhp->m_current_itemsets->get_term_num()
							- 1]->count(iter->first.data());
				}
			}
		}

//		if (dhp->m_current_itemsets->get_term_num() < dhp->m_max_itemset_size) { //对所有候选集进行DHP计数，浪费计算资源，没有必要
		if (dhp->m_current_itemsets->get_term_num() == 1) { //只对2次候选集进行DHP计数
			//DHP哈希计数
			KItemsets first = KItemsets(1);
			for (unsigned int i = 0; i < record.size(); i++) {
				vector<unsigned int> itemset = vector<unsigned int>(
						record.begin() + i, record.begin() + i + 1);
				first.push(itemset, 0);
			}

			KItemsets* current_itemsets = new KItemsets(first);
			KItemsets* new_itemsets = NULL;
			while (current_itemsets->get_term_num()
					< dhp->m_current_itemsets->get_term_num() + 1) {
				new_itemsets = new KItemsets(
						current_itemsets->get_term_num() + 1);
				const map<vector<unsigned int>, unsigned int> itemsets =
						current_itemsets->get_itemsets();
				for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
						itemsets.begin(); iter != itemsets.end(); iter++) {
					const map<vector<unsigned int>, unsigned int> first_itemsets =
							first.get_itemsets();
					for (map<vector<unsigned int>, unsigned int>::const_iterator first_iter =
							first_itemsets.begin();
							first_iter != first_itemsets.end(); first_iter++) {
						vector<unsigned int>* union_itemset =
								KItemsets::union_set(iter->first,
										first_iter->first);
						if (union_itemset->size()
								== new_itemsets->get_term_num()) {
							new_itemsets->push(*union_itemset, 0);
						}
						delete union_itemset;
					}
				}
				delete current_itemsets;
				current_itemsets = new_itemsets;
			}
			const map<vector<unsigned int>, unsigned int> itemsets =
					current_itemsets->get_itemsets();
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					itemsets.begin(); iter != itemsets.end(); iter++) {
				//可以不必去重，因为这样只会增加多余的候选集而不会遗漏，而且去重会有一定消耗
				dhp->m_dhp_counter[dhp->m_current_itemsets->get_term_num()]->count(
						iter->first.data());
			}
			delete current_itemsets;
		}
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
DHP<ItemType, ItemDetail, RecordInfoType>::DHP() {

}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
DHP<ItemType, ItemDetail, RecordInfoType>::~DHP() {

}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool DHP<ItemType, ItemDetail, RecordInfoType>::init(
		unsigned int max_itemset_size, double minsup, double minconf) {
	this->m_max_itemset_size = max_itemset_size;
	this->m_minsup = minsup;
	this->m_minconf = minconf;
	this->destroy_counter();
	this->create_counter();
	this->log = LogUtil::get_instance()->get_log_instance("dhp");
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool DHP<ItemType, ItemDetail, RecordInfoType>::dhp() {
	return this->apriori();
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool DHP<ItemType, ItemDetail, RecordInfoType>::candidate_gen(
		KItemsets& candidate, KItemsets& prv_frq, KItemsets& frq_1) {
	/* 如果是一次频繁项集，进行DHP */
	if (prv_frq.get_term_num() == 1) {
		bind_call_back();
		vector<ItemDetail> temp_item_detail;
		this->m_extractor->set_item_details(&temp_item_detail);
		this->m_extractor->set_record_infos(NULL);
		this->m_extractor->set_items(NULL);
		this->m_current_itemsets = &frq_1;
		this->m_extractor->read_data(false);
	}

	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets =
			prv_frq.get_itemsets();
	const map<vector<unsigned int>, unsigned int>& frq_1_itemsets =
			frq_1.get_itemsets();
	vector<unsigned int>* k_itemset = NULL;

	/* 候选项集生成 */
	for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter =
			prv_frq_itemsets.begin(); prv_frq_iter != prv_frq_itemsets.end();
			prv_frq_iter++) {
		for (map<vector<unsigned int>, unsigned int>::const_iterator frq_1_iter =
				frq_1_itemsets.begin(); frq_1_iter != frq_1_itemsets.end();
				frq_1_iter++) {

			/************************** 项目集连接与过滤 **************************/
			//求并集
			k_itemset = KItemsets::union_set(prv_frq_iter->first,
					frq_1_iter->first);

			/* 剪枝步 */
			//过滤并保存候选项集
			bool filter_result = false;
			if (candidate.get_term_num() == 2) {
				filter_result = this->filter(candidate, prv_frq, k_itemset)
						&& dhp_filter(k_itemset);
			} else {
				filter_result = this->filter(candidate, prv_frq, k_itemset);
			}
			if (filter_result) {
				candidate.push(*k_itemset, 0);
//				this->logItemset("Candidate", k_itemset->size(), *k_itemset);
			}
			delete k_itemset;
		}
	}

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool DHP<ItemType, ItemDetail, RecordInfoType>::dhp_filter(
		vector<unsigned int>* k_itemset) {
	unsigned int minsup_count = double2int(
			this->m_record_infos.size() * this->m_minsup);
	if (0 == minsup_count)
		minsup_count = 1;
	if (m_dhp_counter[this->m_current_itemsets->get_term_num()]->get_count(
			k_itemset->data()) >= minsup_count)
		return true;
	else
		return false;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void DHP<ItemType, ItemDetail, RecordInfoType>::set_extractor(
		Extractor<ItemType, ItemDetail, RecordInfoType>* extractor) {
	this->m_extractor = extractor;
	this->m_extractor->set_record_infos(&this->m_record_infos);
//	this->m_extractor->set_items(&this->m_items); //添加索引记录，apriori使用已存储结构进行统计才开启
	this->m_extractor->set_item_details(&this->m_item_details);
	this->m_extractor->m_assoc = this;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void DHP<ItemType, ItemDetail, RecordInfoType>::bind_call_back() {
	this->m_extractor->set_items_handler(dhp_call_back);
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void DHP<ItemType, ItemDetail, RecordInfoType>::create_counter() {
	for (unsigned int i = 0; i < this->m_max_itemset_size; i++) {
		this->m_itemsets_counter.push_back(
				new HashTableCounter(this->m_item_details.size(), i + 1));
		this->m_dhp_counter.push_back(
				new DHPCounter(this->m_item_details.size(), i + 1));
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void DHP<ItemType, ItemDetail, RecordInfoType>::destroy_counter() {
	for (unsigned int i = 0; i < this->m_itemsets_counter.size(); i++) {
		if (this->m_itemsets_counter[i] != NULL) {
			delete this->m_itemsets_counter[i];
		}
		if (this->m_dhp_counter[i] != NULL) {
			delete this->m_dhp_counter[i];
		}
	}
	this->m_itemsets_counter.clear();
	this->m_dhp_counter.clear();
}

#endif /* DHP_H_ */
