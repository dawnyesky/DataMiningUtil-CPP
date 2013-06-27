/*
 * apriori.h
 *
 *  Created on: 2012-5-7
 *      Author: Yan Shankai
 */

#ifndef APRIORI_H_
#define APRIORI_H_

#include <string.h>
#include <log4cpp/PropertyConfigurator.hh>
#include <log4cpp/FileAppender.hh>
#include "libyskalgrthms/math/digit_util.h"
#include "libyskdmu/util/set_util.h"
#include "libyskdmu/counter/map_triangle_matrix.h"
//#include "libyskdmu/counter/array_triangle_matrix.h"
#include "libyskdmu/association/association_base.h"

using namespace std;

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
class Apriori: public AssocBase<ItemType, ItemDetail, RecordInfoType> {
public:
	Apriori();
	virtual ~Apriori();
	virtual bool init(unsigned int max_itemset_size, double minsup,
			double minconf);

	/*
	 * description: Apriori 频繁项集生成算法
	 *      return: 生成频繁项集是否成功
	 */
	bool apriori();
	/*
	 * description: 候选集生成算法 F(k-1)xF(1)Method
	 *  parameters: candidate：	候选集容器
	 *  			  prv_frq：	(k-1)项频繁项集
	 *  			  frq_1：		1项频繁项集
	 *      return: 生成候选集是否成功
	 */
	virtual bool candidate_gen(KItemsets& candidate, KItemsets& prv_frq,
			KItemsets& frq_1);
	/*
	 * description: Apriori算法潜在频繁项集过滤器，负责剪枝步
	 *  parameters: k_itemset：	需要检查的k次项集
	 *      return: 是否是潜在频繁项集
	 */
	bool filter(KItemsets& potential_frq, KItemsets& prv_frq,
			vector<unsigned int>* k_itemset);
//	friend void count_itemsets(
//			AssocBase<ItemType, ItemDetail, RecordInfoType>* assoc_instance,
//			vector<unsigned int>& record);
	virtual void set_extractor(
			Extractor<ItemType, ItemDetail, RecordInfoType>* extractor);
	virtual unsigned int get_support_count(const vector<unsigned int>& itemset);
	virtual void bind_call_back();
	virtual void create_counter();
	virtual void destroy_counter();
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void apriori_call_back(
		AssocBase<ItemType, ItemDetail, RecordInfoType>* assoc_instance,
		vector<unsigned int>& record) {
	Apriori<ItemType, ItemDetail, RecordInfoType>* apriori = (Apriori<ItemType,
			ItemDetail, RecordInfoType>*) assoc_instance;
	if (NULL == apriori->m_current_itemsets)
		return;
	else {
		const map<vector<unsigned int>, unsigned int>& itemsets =
				apriori->m_current_itemsets->get_itemsets();
		for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
				itemsets.begin(); iter != itemsets.end(); iter++) {
			if (is_subset<unsigned int>(record, iter->first)) {
				apriori->m_itemsets_counter[apriori->m_current_itemsets->get_term_num()
						- 1]->count(iter->first.data());
			}
		}
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
Apriori<ItemType, ItemDetail, RecordInfoType>::Apriori() {

}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
Apriori<ItemType, ItemDetail, RecordInfoType>::~Apriori() {
	this->destroy_counter();
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool Apriori<ItemType, ItemDetail, RecordInfoType>::init(
		unsigned int max_itemset_size, double minsup, double minconf) {
	this->m_max_itemset_size = max_itemset_size;
	this->m_minsup = minsup;
	this->m_minconf = minconf;
	this->destroy_counter();
	this->create_counter();
	this->log = LogUtil::get_instance()->get_log_instance("apriori");
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool Apriori<ItemType, ItemDetail, RecordInfoType>::apriori() {
	// 读取数据集
	this->m_extractor->read_data(false);

	if (this->m_record_infos.size() == 0 || this->m_minsup <= 0
			|| this->m_minconf <= 0) {
		return false;
	}

	unsigned int minsup_count = double2int(
			this->m_record_infos.size() * this->m_minsup);
	if (0 == minsup_count)
		minsup_count = 1;
	vector<unsigned int> itemset;
	KItemsets *frq_itemsets;

	/* F1 generation */
	frq_itemsets = new KItemsets(1);
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		unsigned int support = this->m_extractor->get_counter().at(
				string(this->m_item_details[i].m_identifier));
		if (support >= minsup_count) {
			itemset.clear();
			itemset.push_back(i);
			frq_itemsets->push(itemset, support);
			this->logItemset("Frequent", 1, itemset, support);
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

		KItemsets candidate_itemsets = KItemsets(i + 2);
		if (!candidate_gen(candidate_itemsets, this->m_frequent_itemsets->at(i),
				this->m_frequent_itemsets->at(0))) {
			return false;
		}

		this->m_current_itemsets = &candidate_itemsets;
		frq_itemsets = new KItemsets(i + 2);

		//扫描数据集检查是否频繁
		//没有存储结构start
//		this->m_item_details.clear(); //不可以这样，编译器可能会做优化，把上面HashTable的大小设置为0
//		this->m_extractor->set_item_details(&this->m_item_details);
		bind_call_back();
		vector<ItemDetail> temp_item_detail; //为了不对前一次记录的数据重写，加入一个临时变量，否则handler将无法正确确定item索引值
		this->m_extractor->set_item_details(&temp_item_detail);
		this->m_extractor->set_record_infos(NULL);
		this->m_extractor->set_items(NULL);
		this->m_extractor->read_data(false);
		//没有存储结构end

		//已存储结构start（占用内存较多，不建议使用）
//		for (typename vector<vector<ItemType> >::const_iterator record_iter =
//				this->m_items.begin(); record_iter != this->m_items.end(); record_iter++) {
//			vector<unsigned int> record;
//			for (unsigned int j = 0; j < record_iter->size(); j++) {
//				record.push_back(record_iter->at(j).m_index);
//			}
//			count_itemsets<ItemType, ItemDetail, RecordInfoType>(this, record);
//		}
		//已存储结构end

		// 检查出现次数是否满足最小支持度
		for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
				this->m_current_itemsets->get_itemsets().begin();
				iter != this->m_current_itemsets->get_itemsets().end();
				iter++) {
			unsigned int support =
					this->m_itemsets_counter[this->m_current_itemsets->get_term_num()
							- 1]->get_count(iter->first.data());
			if (support >= minsup_count) {
				itemset.clear();
				for (unsigned int k = 0; k < i + 2; k++) {
					itemset.push_back(iter->first.at(k));
				}
				frq_itemsets->push(itemset, support);
				this->logItemset("Frequent", i + 2, itemset, support);
			}
		}
		if (frq_itemsets->get_itemsets().size() > 0) {
			this->m_frequent_itemsets->push_back(*frq_itemsets);
		}

		delete frq_itemsets;
		this->m_current_itemsets = NULL;
	}
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool Apriori<ItemType, ItemDetail, RecordInfoType>::candidate_gen(
		KItemsets& candidate, KItemsets& prv_frq, KItemsets& frq_1) {
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
			if (filter(candidate, prv_frq, k_itemset)) {
				candidate.push(*k_itemset, 0);
//				this->logItemset("Candidate", k_itemset->size(), *k_itemset, 0);
			}
			delete k_itemset;
		}
	}

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool Apriori<ItemType, ItemDetail, RecordInfoType>::filter(
		KItemsets& potential_frq, KItemsets& prv_frq,
		vector<unsigned int>* k_itemset) {
	//不符合要求的并集
	if (k_itemset->size() != prv_frq.get_term_num() + 1) {
		return false;
	}

	//去重
	if (potential_frq.has_itemset(*k_itemset)) {
		return false;
	}

	/* 剪枝策略 */

	//根据子集闭包性质剪枝
	for (unsigned int k = 0; k < k_itemset->size(); k++) {
		vector<unsigned int> itemset = vector<unsigned int>(*k_itemset);
		vector<unsigned int>::iterator iter = itemset.begin();
		itemset.erase(iter + k);
		if (!prv_frq.has_itemset(itemset)) {
			return false;
		}
	}

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void Apriori<ItemType, ItemDetail, RecordInfoType>::set_extractor(
		Extractor<ItemType, ItemDetail, RecordInfoType>* extractor) {
	this->m_extractor = extractor;
	this->m_extractor->set_record_infos(&this->m_record_infos);
//	this->m_extractor->set_items(&this->m_items); //添加索引记录，apriori使用已存储结构进行统计才开启
	this->m_extractor->set_item_details(&this->m_item_details);
	this->m_extractor->m_assoc = this;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
unsigned int Apriori<ItemType, ItemDetail, RecordInfoType>::get_support_count(
		const vector<unsigned int>& itemset) {
	if (itemset.size() == 1) {
		return this->m_extractor->get_counter().at(
				string(this->m_item_details[itemset[0]].m_identifier));
	} else {
		return this->m_itemsets_counter[this->m_current_itemsets->get_term_num()
				- 1]->get_count(itemset.data());
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void Apriori<ItemType, ItemDetail, RecordInfoType>::bind_call_back() {
	this->m_extractor->set_items_handler(apriori_call_back);
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void Apriori<ItemType, ItemDetail, RecordInfoType>::create_counter() {
	for (unsigned int i = 0; i < this->m_max_itemset_size; i++) {
//		this->m_itemsets_counter.push_back(new HashTableCounter(this->m_item_details.size(), i + 1));
		this->m_itemsets_counter.push_back(new MapTriangleMatrix(this->m_item_details.size()));
//		this->m_itemsets_counter.push_back(new TriangleMatrix(this->m_item_details.size()));
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void Apriori<ItemType, ItemDetail, RecordInfoType>::destroy_counter() {
	for (unsigned int i = 0; i < this->m_itemsets_counter.size(); i++) {
		if (this->m_itemsets_counter[i] != NULL) {
			delete this->m_itemsets_counter[i];
		}
	}
	this->m_itemsets_counter.clear();
}

#endif /* APRIORI_H_ */
