/*
 * association_base.h
 *
 *  Created on: 2012-5-27
 *      Author: "Yan Shankai"
 */

#ifndef ASSOCIATION_BASE_H_
#define ASSOCIATION_BASE_H_

#include <vector>
#include <algorithm>
#include <string.h>
#include "libyskalgrthms/math/arrange_combine.h"
#include "libyskdmu/association/entity/itemset.h"
#include "libyskdmu/counter/hash_table_counter.h"
#include "libyskdmu/association/extractor/extractor_interface.h"

template<typename ItemDetail>
struct AssociationRule {
	vector<ItemDetail> condition;
	vector<ItemDetail> consequent;
	double confidence;
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
class AssocBase {
public:
	AssocBase();
	virtual ~AssocBase();
	virtual bool init(unsigned int max_itemset_size, double minsup,
			double minconf) = 0;
	void set_frequent_itemsets(vector<KItemsets>* frequent_itemsets);
	void set_assoc_rules(vector<AssociationRule<ItemDetail> >* assoc_rules);
	virtual void set_extractor(
			Extractor<ItemType, ItemDetail, RecordInfoType>* extractor) = 0;
	virtual unsigned int get_support_count(
			const vector<unsigned int>& itemset) = 0;
	void enable_log(bool enabled);

	bool genrules();
	void rec_genrules(vector<unsigned int>& frq_itemset,
			vector<vector<unsigned int> >& consequents);

protected:
	void logItemset(const char* type, unsigned int k,
			vector<unsigned int>& itemset, unsigned int support);
	char* print_itemset(vector<unsigned int>& itemset);

public:
	vector<vector<ItemType> > m_items;
	vector<ItemDetail> m_item_details;
	vector<RecordInfoType> m_record_infos;
	vector<KItemsets>* m_frequent_itemsets;
	vector<AssociationRule<ItemDetail> >* m_assoc_rules;
	Extractor<ItemType, ItemDetail, RecordInfoType>* m_extractor;
	vector<Counter*> m_itemsets_counter; //对每次的候选集集合进行统计
	KItemsets* m_current_itemsets;

protected:
	LogInstance* log; //日志指针
	unsigned int m_max_itemset_size; //频繁项集的最大次数
	double m_minsup; //最小支持度
	unsigned int m_minsup_count;
	double m_minconf; //最小置信度
	bool is_inited; //是否已经初始化
	bool enable_log_itemsets; //是否打印频繁项集
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
AssocBase<ItemType, ItemDetail, RecordInfoType>::AssocBase() {
	m_itemsets_counter.clear();
	m_extractor = NULL;
	m_current_itemsets = NULL;
	m_frequent_itemsets = NULL;
	m_assoc_rules = NULL;
	log = NULL;
	enable_log_itemsets = false;
	is_inited = false;
	m_max_itemset_size = 0;
	m_minsup = 0;
	m_minsup_count = 0;
	m_minconf = 0;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
AssocBase<ItemType, ItemDetail, RecordInfoType>::~AssocBase() {
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void AssocBase<ItemType, ItemDetail, RecordInfoType>::set_frequent_itemsets(
		vector<KItemsets>* frequent_itemsets) {
	m_frequent_itemsets = frequent_itemsets;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void AssocBase<ItemType, ItemDetail, RecordInfoType>::set_assoc_rules(
		vector<AssociationRule<ItemDetail> >* assoc_rules) {
	m_assoc_rules = assoc_rules;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void AssocBase<ItemType, ItemDetail, RecordInfoType>::enable_log(bool enabled) {
	this->enable_log_itemsets = enabled;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool AssocBase<ItemType, ItemDetail, RecordInfoType>::genrules() {
	if (NULL == m_assoc_rules) {
		return false;
	}
	if (NULL == m_frequent_itemsets || m_frequent_itemsets->size() == 0) {
		return true;
	}
	for (unsigned int i = 1; i < m_frequent_itemsets->size(); i++) {
		map<vector<unsigned int>, unsigned int> frq_itemsets =
				m_frequent_itemsets->at(i).get_itemsets();
		for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
				frq_itemsets.begin(); iter != frq_itemsets.end(); iter++) {
			//根据每个频繁项集生成关联规则
			vector<vector<unsigned int> >* consequents = new vector<
					vector<unsigned int> >();
			//构造单项集结果集
			for (unsigned int j = 0; j < iter->first.size(); j++) {
				consequents->push_back(
						vector<unsigned int>(iter->first.begin() + j,
								iter->first.begin() + j + 1));
			}
			vector<unsigned int> frq_itemset = vector<unsigned int>(
					iter->first);
			this->rec_genrules(frq_itemset, *consequents);
		}
	}
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void AssocBase<ItemType, ItemDetail, RecordInfoType>::rec_genrules(
		vector<unsigned int>& frq_itemset,
		vector<vector<unsigned int> >& consequents) {
	if (consequents.size() > 0 && frq_itemset.size() > consequents[0].size()) {
		for (vector<vector<unsigned int> >::iterator iter = consequents.begin();
				iter != consequents.end();) {
			//规则格式：f-c=>c，先求出f-c
			vector<unsigned int>* subtract_itemset = KItemsets::subtract_set(
					frq_itemset, *iter);
			//计算f-c=>c的置信度
			this->m_current_itemsets = &(this->m_frequent_itemsets->at(
					frq_itemset.size() - 1)); //get_support_count函数内部需要根据当前所在的频繁项集集合m_current_itemsets来取出计数器
			unsigned int frequent_sup = this->get_support_count(frq_itemset);
			this->m_current_itemsets = &(this->m_frequent_itemsets->at(
					subtract_itemset->size() - 1));
			unsigned int condition_sup = (double) this->get_support_count(
					*subtract_itemset);
			double confidence = (double) frequent_sup / (double) condition_sup;
			if (confidence >= m_minconf) {
				AssociationRule<ItemDetail> assoc_rule;
				for (unsigned int i = 0; i < subtract_itemset->size(); i++) {
					assoc_rule.condition.push_back(
							m_item_details[subtract_itemset->at(i)]);
				}
				for (unsigned int i = 0; i < iter->size(); i++) {
					assoc_rule.consequent.push_back(
							m_item_details[iter->at(i)]);
				}
				assoc_rule.confidence = confidence;
				this->m_assoc_rules->push_back(assoc_rule);
				iter++;
			} else {
				//删除不符合最小置信度的项，下次递归不会再考虑，属于规则剪枝
				iter = consequents.erase(iter);
			}
			delete subtract_itemset;
		}

		//使用自然连接生成下一个结果项集集合
		if (consequents.size() > 0) {
			vector<vector<unsigned int> > next_consequent;
			KItemsets indefinite_size_itemset = KItemsets(
					consequents[0].size() + 1,
					2 * combine(consequents.size(), 2));
			for (unsigned int i = 0; i < consequents.size() - 1; i++) {
				for (unsigned int j = i + 1; j < consequents.size(); j++) {
					vector<unsigned int>* union_itemset = KItemsets::union_set(
							consequents[i], consequents[j]);
					if (union_itemset->size() == consequents[0].size() + 1) {
						indefinite_size_itemset.push(*union_itemset, 0);
					}
					//注意KItemsets::push里是否引用到，是的话不能删除
					delete union_itemset;
				}
			}
			const map<vector<unsigned int>, unsigned int> itemsets =
					indefinite_size_itemset.get_itemsets();
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					itemsets.begin(); iter != itemsets.end(); iter++) {
				next_consequent.push_back(iter->first);
			}

			//递归生成规则
			rec_genrules(frq_itemset, next_consequent);
		}
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void AssocBase<ItemType, ItemDetail, RecordInfoType>::logItemset(
		const char* type, unsigned int k, vector<unsigned int>& itemset,
		unsigned int support) {
	if (this->enable_log_itemsets && log->isDebugEnabled()) {
		char* itemset_str = print_itemset(itemset);
		if (support > 0) {
			log->debug("%s %u-itemsets: { %s }, support count: %u", type, k,
					itemset_str, support);
		} else {
			log->debug("%s %u-itemsets: { %s }", type, k, itemset_str);
		}
		delete itemset_str;
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
char* AssocBase<ItemType, ItemDetail, RecordInfoType>::print_itemset(
		vector<unsigned int>& itemset) {
	char* result = new char[itemset.size() * 100];
	memset(result, '\0', itemset.size() * 100);
	if (itemset.size() > 0) {
		strcat(result, m_item_details[itemset[0]].m_identifier);
		for (unsigned int i = 1; i < itemset.size(); i++) {
			strcat(result, ", ");
			strcat(result, m_item_details[itemset[i]].m_identifier);
		}
	}
	return result;
}

#endif /* ASSOCIATION_BASE_H_ */
