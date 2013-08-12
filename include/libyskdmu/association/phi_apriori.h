/*
 * phi_apriori.h
 *
 *  Created on: 2013-8-5
 *      Author: Yan Shankai
 */

#ifndef PHI_APRIORI_H_
#define PHI_APRIORI_H_

#include <string.h>
#include "libyskdmu/index/distributed_hash_index.h"
#include "libyskdmu/index/mpi_d_hash_index.h"
#include "libyskdmu/association/hi_apriori.h"

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
class ParallelHiApriori: public HiApriori<ItemType, ItemDetail, RecordInfoType> {
public:
	ParallelHiApriori(MPI_Comm comm, unsigned int root_pid = 0,
			unsigned int bucket_size = 10, unsigned int global_deep = 2);
	virtual ~ParallelHiApriori();
	virtual bool init(unsigned int max_itemset_size, double minsup,
			double minconf);

	/*
	 * description: PHI-Apriori 频繁项集生成算法
	 *      return: 生成频繁项集是否成功
	 */
	bool phi_apriori();
	/*
	 * description: 频繁项集生成函数 	F(k-1)xF(k-1)Method
	 *  parameters: frq_itemset：	频繁项集容器
	 *  			 	 prv_frq：		(k-1)项频繁项集
	 *      return: 生成频繁项集是否成功
	 */
	bool phi_frq_gen(KItemsets& frq_itemset, KItemsets& prv_frq1,
			KItemsets& prv_frq2);
	bool phi_genrules();

private:
	bool syn_item_detail();
	pair<void*, int> pack_phig_msg(vector<ItemDetail>& item_detail);
	vector<pair<ItemDetail*, unsigned int> > unpack_phig_msg(
			pair<void*, int> msg_pkg);
	pair<void*, int> pack_phib_msg(char **item_identifiers,
			unsigned int item_id_num, unsigned int *item_detail_offset);
	pair<char**, unsigned int*> unpack_phib_msg(pair<void*, int> msg_pkg);
	pair<void*, int> pack_phis_msg(KItemsets* itemsets);
	KItemsets* unpack_phir_msg(pair<void*, int> msg_pkg);
	pair<void*, int> pack_geng_msg(
			vector<AssociationRule<ItemDetail> >* assoc_rules);
	vector<AssociationRule<ItemDetail> >* unpack_geng_msg(
			pair<void*, int> msg_pkg);
	/*
	 * description: 获取全局项目ID
	 *  parameters: key:		项目关键字
	 *  			key_length:	关键字长度
	 *      return: 本节点所有索引项
	 */
	unsigned int get_global_item_id(const char* key, size_t key_length);
	void toggle_buffer();

public:
	DistributedHashIndex* m_distributed_index; //分布式哈系索引

private:
	MPI_Comm m_comm;
	int m_root_pid;
	unsigned int m_global_item_num;
	char** m_global_identifiers; //全局项目标识
	KItemsets* m_itemset_send_buf;
	KItemsets* m_itemset_recv_buf;
	const static unsigned int PHIG_RECV_BUF_SIZE = 4096;
	const static unsigned int PHIB_BUF_SIZE = 4096;
	const static unsigned int PHIR_BUF_SIZE = 4096;
	const static unsigned int GENG_RECV_BUF_SIZE = 4096;
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::ParallelHiApriori(
		MPI_Comm comm, unsigned int root_pid, unsigned int bucket_size,
		unsigned int global_deep) {
	this->m_item_index = new MPIDHashIndex(comm, bucket_size, global_deep);
	this->m_distributed_index = (DistributedHashIndex*) this->m_item_index;
	this->m_comm = comm;
	this->m_root_pid = root_pid;
	this->m_distributed_index->m_root_pid = root_pid;
	this->m_global_identifiers = NULL;
	this->m_global_item_num = 0;
	this->m_itemset_send_buf = NULL;
	this->m_itemset_recv_buf = NULL;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::~ParallelHiApriori() {
	if (this->m_global_identifiers != NULL && this->m_global_item_num > 0) {
		for (unsigned int i = 0; i < this->m_global_item_num; i++) {
			if (this->m_global_identifiers[i] != NULL) {
				delete[] this->m_global_identifiers[i];
			}
		}
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::init(
		unsigned int max_itemset_size, double minsup, double minconf) {
	this->m_max_itemset_size = max_itemset_size;
	this->m_minsup = minsup;
	this->m_minconf = minconf;
	this->log = LogUtil::get_instance()->get_log_instance("phiApriori");
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_apriori() {
	int pid, numprocs;
	MPI_Comm_size(m_comm, &numprocs);
	MPI_Comm_rank(m_comm, &pid);

	// 读取数据集
	this->m_extractor->read_data(true);

	if (this->m_minsup <= 0 || this->m_minconf <= 0) {
		return false;
	}

	//收集项目详情
	if (!this->syn_item_detail()) {
		return false;
	}

	//同步哈希索引
	if (!this->m_distributed_index->synchronize()) {
		return false;
	}
	if (!this->m_distributed_index->consolidate()) {
		return false;
	}

	this->m_minsup_count = double2int(
			this->m_record_infos.size() * this->m_minsup);
	if (0 == this->m_minsup_count)
		this->m_minsup_count = 1;
	vector<unsigned int> itemset;
	KItemsets *frq_itemsets;
	pair<IndexHead*, int> local_index = m_distributed_index->get_local_index();

	/* F1 generation */
	frq_itemsets = new KItemsets(1);
	for (unsigned int i = 0; i < local_index.second; i++) {
		unsigned int result;
		result = this->m_item_index->get_mark_record_num(
				local_index.first[i].identifier,
				strlen(local_index.first[i].identifier));
		if (result >= this->m_minsup_count) {
			itemset.clear();
			itemset.push_back(
					get_global_item_id(local_index.first[i].identifier,
							strlen(local_index.first[i].identifier)));
			frq_itemsets->push(itemset, result);
			this->logItemset("Frequent", 1, itemset, result);
		}
	}
	if (frq_itemsets->get_itemsets().size() == 0) { //frequent 1-itemsets is not found
		return false;
	}
	this->m_frequent_itemsets->push_back(*frq_itemsets);
	delete frq_itemsets;
	delete[] local_index.first; //不要尝试释放结构里面的东西，因为会影响索引结构

	/* F2~n generation */
	for (unsigned int i = 0;
			this->m_frequent_itemsets->size() == i + 1
					&& i + 1 < this->m_max_itemset_size; i++) {
		frq_itemsets = new KItemsets(i + 2,
				2.5 * combine(this->m_item_details.size(), i + 2));
		//复制当前(k-1)频繁项集到接收缓冲区
		this->m_itemset_recv_buf = new KItemsets(
				this->m_frequent_itemsets->at(i));
		//F(k-1)[本节点]×F(k-1)[本节点]
		if (!phi_frq_gen(*frq_itemsets, this->m_frequent_itemsets->at(i),
				*this->m_itemset_recv_buf)) {
			return false;
		}
		unsigned int comm_times =
				(numprocs % 2 == 0) ? numprocs / 2 - 1 : numprocs / 2;
		unsigned int j = 0;
		for (j = 0; j < comm_times; j++) {
			this->m_itemset_send_buf = new KItemsets();
			this->toggle_buffer();
			//准备接收数据缓冲区
			pair<void*, int> phir_msg_pkg;
			phir_msg_pkg.first = malloc(PHIR_BUF_SIZE);
			phir_msg_pkg.second = PHIR_BUF_SIZE;

			//用发送缓冲区的数据打包发送消息
			pair<void*, int> phis_msg_pkg = this->pack_phis_msg(
					this->m_itemset_send_buf);

			//以非阻塞方式数据发送给下一个节点
			MPI_Bsend(phis_msg_pkg.first, phis_msg_pkg.second, MPI_PACKED,
					(pid + 1) % numprocs, j, m_comm);

			//以阻塞方式从上一节点接收数据
			MPI_Status comm_status;
			MPI_Recv(phir_msg_pkg.first, phir_msg_pkg.second, MPI_PACKED,
					(pid + numprocs - 1) % numprocs, j, m_comm, &comm_status);

			//把接收数据解包到接收缓冲区
			this->m_itemset_recv_buf = unpack_phir_msg(phir_msg_pkg);

			//F(k-1)[本节点]×F(k-1)[上j+1节点]
			if (!phi_frq_gen(*frq_itemsets, this->m_frequent_itemsets->at(i),
					*this->m_itemset_recv_buf)) {
				return false;
			}

			//栅栏同步
			MPI_Barrier(m_comm);

			delete this->m_itemset_send_buf;
		}

		if (numprocs % 2 == 0) {
			if (pid % 2 == 0) { //进程号为偶数的发送
				this->toggle_buffer();
				//用发送缓冲区的数据打包发送消息
				pair<void*, int> phis_msg_pkg = this->pack_phis_msg(
						this->m_itemset_send_buf);
				//以非阻塞方式数据发送给下一个节点
				MPI_Bsend(phis_msg_pkg.first, phis_msg_pkg.second, MPI_PACKED,
						(pid + 1) % numprocs, j, m_comm);
				MPI_Barrier(m_comm);
				this->toggle_buffer();
			} else { //进程号为奇数的接收
				delete this->m_itemset_recv_buf;
				//准备接收数据缓冲区
				pair<void*, int> phir_msg_pkg;
				phir_msg_pkg.first = malloc(PHIR_BUF_SIZE);
				phir_msg_pkg.second = PHIR_BUF_SIZE;

				//以阻塞方式从上一节点接收数据
				MPI_Status comm_status;
				MPI_Recv(phir_msg_pkg.first, phir_msg_pkg.second, MPI_PACKED,
						(pid + numprocs - 1) % numprocs, j, m_comm,
						&comm_status);

				//把接收数据解包到接收缓冲区
				this->m_itemset_recv_buf = unpack_phir_msg(phir_msg_pkg);

				//F(k-1)[本节点]×F(k-1)[上j+1节点]
				if (!phi_frq_gen(*frq_itemsets,
						this->m_frequent_itemsets->at(i),
						*this->m_itemset_recv_buf)) {
					return false;
				}

				//栅栏同步
				MPI_Barrier(m_comm);
			}
		}

		delete this->m_itemset_recv_buf;

		if (frq_itemsets->get_itemsets().size() > 0) {
			this->m_frequent_itemsets->push_back(*frq_itemsets);
		}
		delete frq_itemsets;
	}
	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_frq_gen(
		KItemsets& frq_itemset, KItemsets& prv_frq1, KItemsets& prv_frq2) {
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets1 =
			prv_frq1.get_itemsets();
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets2 =
			prv_frq2.get_itemsets();
	vector<unsigned int>* k_itemset = NULL;

	/* 潜在频繁项集生成 */
	for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter =
			prv_frq_itemsets1.begin(); prv_frq_iter != prv_frq_itemsets1.end();
			prv_frq_iter++) {
		for (map<vector<unsigned int>, unsigned int>::const_iterator frq_1_iter =
				prv_frq_itemsets2.begin();
				frq_1_iter != prv_frq_itemsets2.end(); frq_1_iter++) {

			/************************** 项目集连接与过滤 **************************/
			//求并集
			k_itemset = KItemsets::union_set(prv_frq_iter->first,
					frq_1_iter->first);

			//过滤并保存潜在频繁项集
			unsigned int support;
			if (this->hi_filter(k_itemset, &support)) {
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
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_genrules() {
	AssocBase<ItemType, ItemDetail, RecordInfoType>::genrules();
	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);

	//打包Gather消息
	pair<void*, int> geng_send_msg_pkg = pack_geng_msg(this->m_assoc_rules);

	//准备Gather消息缓冲区
	pair<void*, int> geng_recv_msg_pkg;
	geng_recv_msg_pkg.first = NULL;
	geng_recv_msg_pkg.second = 0;
	if (pid == m_root_pid) {
		geng_recv_msg_pkg.first = malloc(numprocs * GENG_RECV_BUF_SIZE);
		geng_recv_msg_pkg.second = GENG_RECV_BUF_SIZE;
	}

	MPI_Gather(geng_send_msg_pkg.first, geng_recv_msg_pkg.second, MPI_PACKED,
			geng_recv_msg_pkg.first, geng_recv_msg_pkg.second, MPI_PACKED,
			m_root_pid, m_comm);

	//处理汇总数据
	if (pid == m_root_pid) {
		//解包Gather消息
		vector<AssociationRule<ItemDetail> >* geng_recv_msg = unpack_geng_msg(
				geng_recv_msg_pkg);
		for (unsigned int i = 0; i < geng_recv_msg->size(); i++) {
			this->m_assoc_rules->push_back(geng_recv_msg->at(i));
		}

		delete geng_recv_msg;
	}

	free(geng_send_msg_pkg.first);
	if (pid == m_root_pid) {
		free(geng_recv_msg_pkg.first);
	}

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::syn_item_detail() {
	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);

	//准备Gather接收的数据缓冲区
	pair<void*, int> phig_recv_msg_pkg;
	phig_recv_msg_pkg.first = NULL;
	phig_recv_msg_pkg.second = 0;
	if (pid == m_root_pid) {
		phig_recv_msg_pkg.first = malloc(numprocs * PHIG_RECV_BUF_SIZE);
		phig_recv_msg_pkg.second = PHIG_RECV_BUF_SIZE;
	}

	//打包Gather数据
	pair<void*, int> phig_send_msg_pkg = pack_phig_msg(this->m_item_details);

	MPI_Gather(phig_send_msg_pkg.first, phig_send_msg_pkg.second, MPI_PACKED,
			phig_recv_msg_pkg.first, phig_send_msg_pkg.second, MPI_PACKED,
			m_root_pid, m_comm);

	//声明Broadcast数据包
	pair<void*, int> phib_msg_pkg;

	if (pid == m_root_pid) {
		//解包Gather数据
		vector<pair<ItemDetail*, unsigned int> > phig_recv_msg =
				unpack_phig_msg(phig_recv_msg_pkg);

		//处理Gather数据
		unsigned int item_detail_offset[numprocs]; //各个进程中项目详情在全局中的起始ID
		item_detail_offset[0] = 0;
		unsigned int item_num = phig_recv_msg[0].second; //全局项目详情数量
		assert(phig_recv_msg.size() == numprocs);
		for (unsigned int i = 1; i < phig_recv_msg.size(); i++) {
			item_detail_offset[i] = item_detail_offset[i - 1]
					+ phig_recv_msg[i - 1].second;
			item_num += phig_recv_msg[i].second;
		}
		char* old_item_identifiers[this->m_item_details.size()]; //根进程原有项目标识
		unsigned int old_item_num = this->m_item_details.size();
		//清空原有数据
		for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
			old_item_identifiers[i] = this->m_item_details[i].m_identifier;
			this->m_item_details[i].m_identifier = NULL;
		}
		this->m_item_details.clear();

		//更新索引的key_info为全局信息
		for (unsigned int i = 0; i < old_item_num; i++) {
			unsigned int gid = item_detail_offset[pid] + i;
			char* gid_str = itoa(gid);
			char* key_info = NULL;
			this->m_item_index->get_key_info(&key_info, old_item_identifiers[i],
					strlen(old_item_identifiers[i]));
			char* new_key_info =
					new char[strlen(key_info) + strlen(gid_str) + 2];
			strcpy(new_key_info, key_info);
			strcat(new_key_info, "#");
			strcat(new_key_info, gid_str);
			this->m_item_index->change_key_info(old_item_identifiers[i],
					strlen(old_item_identifiers[i]), new_key_info);
			delete[] gid_str;
			delete[] new_key_info;
		}

		//更新全局项目标识和准备Broadcast数据
		char* item_identifiers[item_num];
		for (unsigned int i = 0, index = 0; i < phig_recv_msg.size(); i++) {
			for (unsigned int j = 0; j < phig_recv_msg[i].second; j++) {
				this->m_item_details.push_back(phig_recv_msg[i].first[j]); //加入新数据
				item_identifiers[index] =
						this->m_item_details[index].m_identifier; //准备Broadcast数据
				index++;
			}
			delete[] phig_recv_msg[i].first; //清除接收数据
		}

		//打包Broadcast数据
		phib_msg_pkg = pack_phib_msg(item_identifiers, item_num,
				item_detail_offset);
	}

	//打包Broadcast数据
	phib_msg_pkg = pack_phib_msg(NULL, 0, NULL);

	MPI_Bcast(phib_msg_pkg.first, phib_msg_pkg.second, MPI_PACKED, m_root_pid,
			m_comm);

	//解包Broadcast数据
	pair<char**, unsigned int*> phib_msg = unpack_phib_msg(phib_msg_pkg);

	//处理Broadcast数据
	if (pid != m_root_pid) {
		this->m_global_item_num = phib_msg.second[0];
		this->m_global_identifiers = new char*[m_global_item_num];
		for (unsigned int i = 0; i < phib_msg.second[0]; i++) {
			//写入全局项目标识列表方便查询
			this->m_global_identifiers[i] = phib_msg.first[i];
		}

		//更新索引的key_info为全局信息
		for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
			unsigned int gid = *(phib_msg.second + pid + 1) + i;
			char* gid_str = itoa(gid);
			assert(
					strcmp(this->m_item_details[i].m_identifier,
							phib_msg.first[gid]) == 0);
			char* key_info = NULL;
			this->m_item_index->get_key_info(&key_info, phib_msg.first[gid],
					strlen(phib_msg.first[gid]));
			char* new_key_info =
					new char[strlen(key_info) + strlen(gid_str) + 2];
			strcpy(new_key_info, key_info);
			strcat(new_key_info, "#");
			strcat(new_key_info, gid_str);
			this->m_item_index->change_key_info(phib_msg.first[i],
					strlen(phib_msg.first[i]), new_key_info);
			delete[] gid_str;
			delete[] new_key_info;
		}
	}

	free(phig_send_msg_pkg.first);
	free(phig_recv_msg_pkg.first);

	delete[] phib_msg.second;
	free(phib_msg_pkg.first);

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_phig_msg(
		vector<ItemDetail>& item_detail) {
	pair<void*, int> result;

	//计算缓冲区大小：
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &result.second);
	for (unsigned int i = 0; i < item_detail.size(); i++) {
		result.second += item_detail[i].get_mpi_pack_size(m_comm);
	}

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	int position = 0;
	unsigned int item_num = item_detail.size();
	MPI_Pack(&item_num, 1, MPI_UNSIGNED, result.first, result.second, &position,
			m_comm);
	for (unsigned int i = 0; i < item_num; i++) {
		pair<void*, int> item_pkg = item_detail[i].mpi_pack(m_comm);
		MPI_Pack(item_pkg.first, item_pkg.second, MPI_PACKED, result.first,
				result.second, &position, m_comm);
		free(item_pkg.first);
	}

	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
vector<pair<ItemDetail*, unsigned int> > ParallelHiApriori<ItemType, ItemDetail,
		RecordInfoType>::unpack_phig_msg(pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	MPI_Comm_size(this->m_comm, &numprocs);
	vector<pair<ItemDetail*, unsigned int> > result;
	for (unsigned int i = 0; i < numprocs; i++) {
		pair<ItemDetail*, unsigned int> result_item;
		position = i * PHIG_RECV_BUF_SIZE;
		unsigned int item_num = 0;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &item_num, 1,
				MPI_UNSIGNED, m_comm);
		result_item.second = item_num;
		result_item.first = new ItemDetail[item_num];
		ItemDetail::mpi_unpack(msg_pkg.first, msg_pkg.second, &position,
				result_item.first, item_num, m_comm);
		result.push_back(result_item);
	}
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_phib_msg(
		char **item_identifiers, unsigned int item_id_num,
		unsigned int *item_detail_offset) {
	int numprocs;
	MPI_Comm_size(m_comm, &numprocs);
	pair<void*, int> result;

	//计算缓冲区大小：
	int fixed_uint_num = 1 + item_id_num + numprocs; //项目标识数量，每个项目标识的长度，每个项目详情ID的偏移
	int dynamic_char_num = 0;
	for (unsigned int i = 0; i < item_id_num; i++) {
		dynamic_char_num += strlen(item_identifiers[i]) + 1; //每个项目标识
	}
	int fixed_uint_size, dynamic_char_size;
	MPI_Pack_size(fixed_uint_num, MPI_UNSIGNED, m_comm, &fixed_uint_size);
	MPI_Pack_size(dynamic_char_num, MPI_CHAR, m_comm, &dynamic_char_size);
	result.second = fixed_uint_size + dynamic_char_size;
	assert(result.second <= PHIB_BUF_SIZE); //断言打包的数据比设定的缓冲区小
	result.second = PHIB_BUF_SIZE;

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	if (item_identifiers != NULL && item_detail_offset != NULL
			&& item_id_num != 0) {
		int position = 0;
		MPI_Pack(&item_id_num, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		for (unsigned int i = 0; i < item_id_num; i++) {
			unsigned int item_id_len = strlen(item_identifiers[i]) + 1;
			MPI_Pack(&item_id_len, 1, MPI_UNSIGNED, result.first, result.second,
					&position, m_comm);
			MPI_Pack(item_identifiers + i, item_id_len, MPI_CHAR, result.first,
					result.second, &position, m_comm);
		}
		MPI_Pack(item_detail_offset, numprocs, MPI_UNSIGNED, result.first,
				result.second, &position, m_comm);
	}
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<char**, unsigned int*> ParallelHiApriori<ItemType, ItemDetail,
		RecordInfoType>::unpack_phib_msg(pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	MPI_Comm_size(m_comm, &numprocs);
	pair<char**, unsigned int*> result;

	unsigned int item_id_num;
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &item_id_num, 1,
			MPI_UNSIGNED, m_comm);

	result.first = new char*[item_id_num];
	result.second = new unsigned int[numprocs + 1];
	result.second[0] = item_id_num;

	for (unsigned int i = 0; i < item_id_num; i++) {
		unsigned int item_id_len = 0;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &item_id_len, 1,
				MPI_UNSIGNED, m_comm);
		result.first[i] = new char[item_id_len];
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, result.first[i],
				item_id_len, MPI_CHAR, m_comm);
	}
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, result.second + 1,
			numprocs, MPI_UNSIGNED, m_comm);
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_phis_msg(
		KItemsets* itemsets) {
	pair<void*, int> result;

	unsigned int term_num = itemsets->get_term_num();
	unsigned int itemset_num = itemsets->get_itemsets().size();
	//计算缓冲区大小：
	MPI_Pack_size(2 + itemset_num * (term_num + 1), MPI_UNSIGNED, m_comm,
			&result.second); //项集次数 + 项集的个数 + 项集个数×(项集+支持度)

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	int position = 0;
	MPI_Pack(&term_num, 1, MPI_UNSIGNED, result.first, result.second, &position,
			m_comm);
	MPI_Pack(&itemset_num, 1, MPI_UNSIGNED, result.first, result.second,
			&position, m_comm);
	map<vector<unsigned int>, unsigned int> itemsetss =
			itemsets->get_itemsets();
	for (map<vector<unsigned int>, unsigned int>::const_iterator iter =
			itemsetss.begin(); iter != itemsetss.end(); iter++) {
		for (unsigned int j = 0; j < term_num; j++) {
			unsigned int item = iter->first[j];
			MPI_Pack(&item, 1, MPI_UNSIGNED, result.first, result.second,
					&position, m_comm);
		}
		unsigned int support = iter->second;
		MPI_Pack(&support, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
	}
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
KItemsets* ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::unpack_phir_msg(
		pair<void*, int> msg_pkg) {
	int position = 0;
	unsigned int term_num, itemset_num;
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &term_num, 1,
			MPI_UNSIGNED, m_comm);
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &itemset_num, 1,
			MPI_UNSIGNED, m_comm);
	KItemsets* result = new KItemsets(term_num, 2.5 * itemset_num);
	for (unsigned int i = 0; i < itemset_num; i++) {
		vector<unsigned int> itemset;
		for (unsigned int j = 0; j < term_num; j++) {
			unsigned int item;
			MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &item, 1,
					MPI_UNSIGNED, m_comm);
			itemset.push_back(item);
		}
		unsigned int support;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &support, 1,
				MPI_UNSIGNED, m_comm);
		result->push(itemset, support);
	}
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_geng_msg(
		vector<AssociationRule<ItemDetail> >* assoc_rules) {
	pair<void*, int> result;

	//计算缓冲区大小：
	int assoc_num_mpi_size, conf_mpi_size, item_detail_num_mpi_size,
			item_detail_mpi_size = 0;
	int conf_num = assoc_rules->size();
	int item_detail_num = 2 * assoc_rules->size();
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &assoc_num_mpi_size);
	MPI_Pack_size(conf_num, MPI_DOUBLE, m_comm, &conf_mpi_size);
	MPI_Pack_size(item_detail_num, MPI_UNSIGNED, m_comm,
			&item_detail_num_mpi_size);
	for (unsigned int i = 0; i < assoc_rules->size(); i++) {
		for (unsigned int j = 0; j < assoc_rules->at(i).condition.size(); j++) {
			item_detail_mpi_size +=
					assoc_rules->at(i).condition[j].get_mpi_pack_size(m_comm);
		}

		for (unsigned int j = 0; j < assoc_rules->at(i).consequent.size();
				j++) {
			item_detail_mpi_size +=
					assoc_rules->at(i).consequent[j].get_mpi_pack_size(m_comm);
		}
	}
	result.second = assoc_num_mpi_size + conf_mpi_size
			+ item_detail_num_mpi_size + item_detail_mpi_size; //规则数量，每条规则的信任度，每条规则的条件和结果的数量，每条规则的条件和结果

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	int position = 0;
	unsigned int rule_num = assoc_rules->size();
	MPI_Pack(&rule_num, 1, MPI_UNSIGNED, result.first, result.second, &position,
			m_comm);
	for (unsigned int i = 0; i < assoc_rules->size(); i++) {
		unsigned int cond_num = assoc_rules->at(i).condition.size();
		MPI_Pack(&cond_num, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		for (unsigned int j = 0; j < assoc_rules->at(i).condition.size(); j++) {
			pair<void*, int> cond_pkg =
					assoc_rules->at(i).condition[j].mpi_pack(m_comm);
			MPI_Pack(cond_pkg.first, cond_pkg.second, MPI_PACKED, result.first,
					result.second, &position, m_comm);
			free(cond_pkg.first);
		}

		unsigned int cons_num = assoc_rules->at(i).consequent.size();
		MPI_Pack(&cons_num, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		for (unsigned int j = 0; j < assoc_rules->at(i).consequent.size();
				j++) {
			pair<void*, int> cons_pkg =
					assoc_rules->at(i).consequent[j].mpi_pack(m_comm);
			MPI_Pack(cons_pkg.first, cons_pkg.second, MPI_PACKED, result.first,
					result.second, &position, m_comm);
			free(cons_pkg.first);
		}
		double confidence = assoc_rules->at(i).confidence;
		MPI_Pack(&confidence, 1, MPI_DOUBLE, result.first, result.second,
				&position, m_comm);
	}

	return result;
}
template<typename ItemType, typename ItemDetail, typename RecordInfoType>
vector<AssociationRule<ItemDetail> >* ParallelHiApriori<ItemType, ItemDetail,
		RecordInfoType>::unpack_geng_msg(pair<void*, int> msg_pkg) {
	int position = 0;
	unsigned int rule_num, cond_num, cons_num;
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &rule_num, 1,
			MPI_UNSIGNED, m_comm);
	vector<AssociationRule<ItemDetail> >* result = new vector<
			AssociationRule<ItemDetail> >();
	for (unsigned int i = 0; i < rule_num; i++) {
		AssociationRule<ItemDetail> assoc_rule;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &cond_num, 1,
				MPI_UNSIGNED, m_comm);
		ItemDetail condition[cond_num];
		ItemDetail::mpi_unpack(msg_pkg.first, msg_pkg.second, &position,
				condition, cond_num, m_comm);
		for (unsigned int j = 0; j < cond_num; j++) {
			assoc_rule.condition.push_back(condition[j]);
		}

		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &cons_num, 1,
				MPI_UNSIGNED, m_comm);
		ItemDetail consequent[cons_num];
		ItemDetail::mpi_unpack(msg_pkg.first, msg_pkg.second, &position,
				consequent, cons_num, m_comm);
		for (unsigned int j = 0; j < cons_num; j++) {
			assoc_rule.consequent.push_back(consequent[j]);
		}

		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position,
				&assoc_rule.confidence, 1, MPI_DOUBLE, m_comm);

		result->push_back(assoc_rule);
	}
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
unsigned int ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::get_global_item_id(
		const char* key, size_t key_length) {
	if (!this->m_distributed_index->is_consolidated) {
		if (!this->m_distributed_index->is_synchronized) {
			if (!this->m_distributed_index->synchronize())
				exit(-1);
		}
		if (!this->m_distributed_index->consolidate()) {
			exit(-1);
		}
	}

	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);

	char* key_info;
	this->m_item_index->get_key_info(&key_info, key, key_length);
	//从pid#lid#gid分割出gid
	char* p[4];
	char* buf = key_info;
	int in = 0;
	while ((p[in] = strtok(buf, "#")) != NULL) {
		in++;
		buf = NULL;
	}
	unsigned int result = ysk_atoi(p[2], strlen(p[2]));
	delete[] key_info;
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
void ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::toggle_buffer() {
	KItemsets* temp = this->m_itemset_send_buf;
	this->m_itemset_send_buf = this->m_itemset_recv_buf;
	this->m_itemset_recv_buf = temp;
}

#endif /* PHI_APRIORI_H_ */
