/*
 * fp_growth.h
 *
 *  Created on: 2012-5-27
 *      Author: "Yan Shankai"
 */

#ifndef FP_GROWTH_H_
#define FP_GROWTH_H_

#include <vector>
#include <string.h>
#include "libyskalgrthms/util/string.h"
#include "libyskalgrthms/math/arrange_combine.h"
#include "libyskalgrthms/math/digit_util.h"
#include "libyskalgrthms/sort/quicksort_tmplt.h"
#include "libyskdmu/association/association_base.h"

using namespace std;

struct FpTreeNode {
	vector<FpTreeNode*> m_child;
	FpTreeNode* m_same;
	unsigned int* m_key;
	unsigned int m_count;

	FpTreeNode() {
		m_key = NULL;
		m_same = NULL;
		m_count = 0;
	}

	FpTreeNode(unsigned int key) {
		m_key = new unsigned int(key);
		m_same = NULL;
		m_count = 1;
	}
	string print_tree(FpTreeNode* sub_tree) {
		char* key_str = NULL;
		char* count_str = NULL;
		if (0 == sub_tree->m_child.size()) {
			if (sub_tree->m_key != NULL) {
				key_str = itoa(*(sub_tree->m_key), 10);
				count_str = itoa(sub_tree->m_count, 10);
			} else {
				key_str = new char[7];
				strcpy(key_str, "\"NULL\"");
				strcat(key_str, "\0");
				count_str = new char[2];
				strcpy(count_str, "0\0");
			}
			string result = "{\"key\":" + string(key_str) + ",\"num\":"
					+ string(count_str) + "}";
			delete[] count_str;
			delete[] key_str;
			return result;

		} else {
			if (sub_tree->m_key != NULL) {
				key_str = itoa(*(sub_tree->m_key), 10);
				count_str = itoa(sub_tree->m_count, 10);
			} else {
				key_str = new char[7];
				strcpy(key_str, "\"NULL\"");
				strcat(key_str, "\0");
				count_str = new char[2];
				strcpy(count_str, "0\0");
			}
			string result = "{\"key\":" + string(key_str) + ",\"num\":"
					+ string(count_str) + ",\"child\":[";
			unsigned int i = 0;
			for (i = 0; i < sub_tree->m_child.size() - 1; i++) {
				result += print_tree(sub_tree->m_child[i]);
				result += ", ";
			}
			result += (print_tree(sub_tree->m_child[i]) + "]}");
			delete[] count_str;
			delete[] key_str;
			return result;

		}
	}
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
class FpGrowth: public AssocBase<ItemType, ItemDetail, RecordInfoType> {
public:
	FpGrowth();
	virtual ~FpGrowth();
	bool init(unsigned int max_itemset_size, double minsup, double minconf);
	bool fp_growth();
	void rec_fp_growth(FpTreeNode* sub_tree, vector<KItemsets*> pattern_base);
	virtual void set_extractor(
			Extractor<ItemType, ItemDetail, RecordInfoType>* extractor);
	void bind_call_back();
	virtual unsigned int get_support_count(const vector<unsigned int>& itemset);
	const vector<unsigned int> get_sorted_index() const;

protected:
	void sort_items_index();

public:
	FpTreeNode* m_fp_tree;

protected:
	vector<unsigned int> m_sorted_item_index;
	vector<vector<KItemsets*> > m_pattern_base;
	vector<vector<HashTableCounter*> > m_pattern_counter;
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void rec_insert_tree(FpGrowth<ItemType, ItemDetail, RecordInfoType>* fp_growth,
		vector<unsigned int>& keys, FpTreeNode* fp_tree) {
	FpTreeNode* sub_fp_tree = NULL;
	for (unsigned int i = 0; i < fp_tree->m_child.size(); i++) {
		if (*(fp_tree->m_child[i]->m_key) == keys[0]) {
			fp_tree->m_child[i]->m_count++;
			sub_fp_tree = fp_tree->m_child[i];
		}
	}
	if (NULL == sub_fp_tree) {
		FpTreeNode* new_node = new FpTreeNode(keys[0]);
		sub_fp_tree = new_node;
		fp_tree->m_child.push_back(new_node);
	}
	// 考察剩余的元素，若非空即递归调用insert_tree
	keys.erase(keys.begin());
	if (keys.size() > 0) {
		rec_insert_tree(fp_growth, keys, sub_fp_tree);
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void insert_tree(FpGrowth<ItemType, ItemDetail, RecordInfoType>* fp_growth,
		vector<unsigned int>& keys) {
	rec_insert_tree(fp_growth, keys, fp_growth->m_fp_tree);
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void fp_call_back(
		AssocBase<ItemType, ItemDetail, RecordInfoType>* assoc_instance,
		vector<unsigned int>& record) {
	FpGrowth<ItemType, ItemDetail, RecordInfoType>* fp_growth = (FpGrowth<
			ItemType, ItemDetail, RecordInfoType>*) assoc_instance;
	unsigned int record_array[record.size()];
	for (unsigned int j = 0; j < record.size(); j++) {
		record_array[j] = record.at(j);
	}
	vector<unsigned int> sorted_index = fp_growth->get_sorted_index();
	unsigned int count_array[sorted_index.size()];
	for (unsigned int k = 0; k < sorted_index.size(); k++) {
		count_array[sorted_index[k]] = sorted_index.size() - k;
	}
	unsigned int counts[record.size()];
	for (unsigned int l = 0; l < record.size(); l++) {
		counts[l] = count_array[record_array[l]];
	}
	//让记录索引按照一次频繁项的计数值降序排列
	quicksort<unsigned int>(counts, record.size(), false, false, record_array);
	vector<unsigned int> v_record;
	for (unsigned int m = 0; m < record.size(); m++) {
		v_record.push_back(record_array[m]);
	}
	insert_tree<ItemType, ItemDetail, RecordInfoType>(fp_growth, v_record);
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
FpGrowth<ItemType, ItemDetail, RecordInfoType>::FpGrowth() {
	this->m_current_itemsets = NULL;
	m_fp_tree = NULL;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
FpGrowth<ItemType, ItemDetail, RecordInfoType>::~FpGrowth() {
	for (unsigned int i = 0; i < m_pattern_base.size(); i++) {
		for (unsigned int j = 0; j < m_pattern_base[i].size(); j++) {
			if (m_pattern_base[i][j] != NULL) {
				delete m_pattern_base[i][j];
			}
		}
	}
	for (unsigned int i = 0; i < m_pattern_counter.size(); i++) {
		for (unsigned int j = 0; j < m_pattern_counter[i].size(); j++) {
			if (m_pattern_counter[i][j] != NULL) {
				delete m_pattern_counter[i][j];
			}
		}
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool FpGrowth<ItemType, ItemDetail, RecordInfoType>::init(
		unsigned int max_itemset_size, double minsup, double minconf) {
	this->m_max_itemset_size = max_itemset_size;
	this->m_minsup = minsup;
	this->m_minconf = minconf;
	this->log = LogUtil::get_instance()->get_log_instance("fpGrowth");
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool FpGrowth<ItemType, ItemDetail, RecordInfoType>::fp_growth() {
	// 读取数据集
	this->m_extractor->read_data(false);

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
		unsigned int support = this->m_extractor->get_counter().at(
				string(this->m_item_details[i].m_identifier));
		if (support >= this->m_minsup_count) {
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
	//对一次频繁项集降序排列
	sort_items_index();

	//第二次扫描数据集，构造FP-Tree
	m_fp_tree = new FpTreeNode();
	m_fp_tree->m_key = NULL;

	bind_call_back();
	vector<ItemDetail> temp_item_detail; //为了不对前一次记录的数据重写，加入一个临时变量，否则handler将无法正确确定item索引值
	this->m_extractor->set_item_details(&temp_item_detail);
	this->m_extractor->set_record_infos(NULL);
	this->m_extractor->set_items(NULL);
	this->m_extractor->read_data(false);

	//对FP-Tree进行挖掘，找出所有频繁项集
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		vector<KItemsets*> k_itemsets;
		for (unsigned int i = 0; i < this->m_max_itemset_size - 1; i++) {
			//初始化KItemsets需要谨慎，这里取的是平均值的1.5倍，太大会占用过多内存
			k_itemsets.push_back(
					new KItemsets(i + 1,
							1.5 * combine(this->m_item_details.size(), i + 1)));
		}
		m_pattern_base.push_back(k_itemsets);
	}
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		vector<HashTableCounter*> pattern_counter;
		for (unsigned int i = 0; i < this->m_max_itemset_size - 1; i++) {
			pattern_counter.push_back(
					new HashTableCounter(this->m_item_details.size(), i + 1));
		}
		m_pattern_counter.push_back(pattern_counter);
	}

	vector<KItemsets*> k_itemsets;
	for (unsigned int i = 0; i < this->m_max_itemset_size - 1; i++) {
		k_itemsets.push_back(
				new KItemsets(i + 1,
						1.5 * combine(this->m_item_details.size(), i + 1)));
	}

	//此处的k_itemsets代表父/祖父节点排列组合成的频繁模式基
	rec_fp_growth(m_fp_tree, k_itemsets);

	KItemsets* itemsets = NULL;
	//遍历频繁模式基表，把每个频繁模式基与后缀连接。
	for (unsigned int i = 0; i < this->m_max_itemset_size - 1; i++) {
		itemsets = new KItemsets(i + 2);
		for (unsigned int j = 0; j < this->m_item_details.size(); j++) {
			vector<unsigned int> current;
			current.push_back(j);
			map<vector<unsigned int>, unsigned int> pattern_base =
					m_pattern_base[j][i]->get_itemsets();
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					pattern_base.begin(); iter != pattern_base.end(); iter++) {
				unsigned int support = m_pattern_counter[j][i]->get_count(
						iter->first.data());
				if (support >= this->m_minsup_count) {
					vector<unsigned int>* union_itemset = KItemsets::union_set(
							iter->first, current);
					if (union_itemset->size() == i + 2) {
						itemsets->push(*union_itemset, support);
						this->logItemset("Frequent", i + 2, *union_itemset,
								support);
					}
				}
			}
		}
		if (itemsets->get_itemsets().size() > 0) {
			this->m_frequent_itemsets->push_back(*itemsets);
		}
		delete itemsets;
	}
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void FpGrowth<ItemType, ItemDetail, RecordInfoType>::rec_fp_growth(
		FpTreeNode* sub_tree, vector<KItemsets*> pattern_base) {
	if (sub_tree->m_key != NULL) {
		// 把pattern_base加入当前节点的频繁模式基中
		vector<unsigned int> current = vector<unsigned int>(sub_tree->m_key,
				sub_tree->m_key + 1);
		for (unsigned int i = 0; i < pattern_base.size(); i++) {
			map<vector<unsigned int>, unsigned int> itemsets =
					pattern_base[i]->get_itemsets();
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					itemsets.begin(); iter != itemsets.end(); iter++) {
				vector<unsigned int> current_itemset = iter->first;
				if (!m_pattern_base[*(sub_tree->m_key)][i]->has_itemset(
						current_itemset)) {
					m_pattern_base[*(sub_tree->m_key)][i]->push(current_itemset,
							0);
				}
				m_pattern_counter[*(sub_tree->m_key)][i]->count(
						current_itemset.data(), sub_tree->m_count);
			}
		}

		// 把当前节点与pattern_base(除了最后一维)里的每个元素进行连接合并
		for (unsigned int i = 0; i < pattern_base.size() - 1; i++) {
			map<vector<unsigned int>, unsigned int> itemsets =
					pattern_base[i]->get_itemsets();
			for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
					itemsets.begin(); iter != itemsets.end(); iter++) {
				vector<unsigned int>* union_itemset = KItemsets::union_set(
						iter->first, current);
				if (!pattern_base[union_itemset->size() - 1]->has_itemset(
						current)) {
					pattern_base[union_itemset->size() - 1]->push(
							*union_itemset, 0); //此处的0不代表任何意义，只是不需要这个参数
				}
				delete union_itemset;
			}
		}
		// 把当前节点并入pattern_base
		if (!pattern_base[0]->has_itemset(current)) {
			pattern_base[0]->push(current, 0);
		}
	}

// 对每个子节点递归进行挖掘
	for (unsigned int i = 0; i < sub_tree->m_child.size(); i++) {
		rec_fp_growth(sub_tree->m_child[i], pattern_base);
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void FpGrowth<ItemType, ItemDetail, RecordInfoType>::set_extractor(
		Extractor<ItemType, ItemDetail, RecordInfoType>* extractor) {
	this->m_extractor = extractor;
	this->m_extractor->set_record_infos(&this->m_record_infos);
	this->m_extractor->set_items(&this->m_items);
	this->m_extractor->set_item_details(&this->m_item_details);
	this->m_extractor->m_assoc = this;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void FpGrowth<ItemType, ItemDetail, RecordInfoType>::bind_call_back() {
	this->m_extractor->set_items_handler(fp_call_back);
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
unsigned int FpGrowth<ItemType, ItemDetail, RecordInfoType>::get_support_count(
		const vector<unsigned int>& itemset) {
	if (itemset.size() == 1) {
		return this->m_extractor->get_counter().at(
				string(this->m_item_details[itemset[0]].m_identifier));
	} else {
		vector<unsigned int> pattern_base = vector<unsigned int>(
				itemset.begin(), itemset.end() - 1);
		return m_pattern_counter[itemset.back()][itemset.size() - 2]->get_count(
				pattern_base.data());
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void FpGrowth<ItemType, ItemDetail, RecordInfoType>::sort_items_index() {
	unsigned int counts[this->m_item_details.size()];
	unsigned int index[this->m_item_details.size()];
	map<string, unsigned int> counter = this->m_extractor->get_counter();
	for (std::map<string, unsigned int>::const_iterator iter = counter.begin();
			iter != counter.end(); iter++) {
		counts[this->m_extractor->m_index.at(iter->first)] = iter->second;
	}
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		index[i] = i;
	}
	quicksort<unsigned int>(counts, this->m_item_details.size(), true, false,
			index);
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		this->m_sorted_item_index.push_back(
				index[this->m_item_details.size() - 1 - i]);
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
const vector<unsigned int> FpGrowth<ItemType, ItemDetail, RecordInfoType>::get_sorted_index() const {
	return m_sorted_item_index;
}

#endif /* FP_GROWTH_H_ */
