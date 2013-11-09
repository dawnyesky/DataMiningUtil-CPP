/*
 * phi_apriori.h
 *
 *  Created on: 2013-8-5
 *      Author: Yan Shankai
 */

#ifndef PHI_APRIORI_H_
#define PHI_APRIORI_H_

#include <math.h>
#include <string.h>
#include <omp.h>
#include "libyskdmu/index/distributed_hash_index.h"
#include "libyskdmu/index/mpi_d_hash_index.h"
#include "libyskdmu/index/ro_dynamic_hash_index.h"
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
	bool phi_genrules();

protected:
	bool phi_frq_gen(KItemsets& frq_itemset, KItemsets& prv_frq1,
			KItemsets& prv_frq2);
	bool phi_filter(vector<unsigned int>* k_itemset, unsigned int* support);
	bool gen_ro_index();
	bool destroy_ro_index();

private:
	bool syn_item_detail();
	bool syn_record_info();
	pair<void*, int> pack_synidg_msg(vector<ItemDetail>& item_details);
	vector<pair<ItemDetail*, unsigned int> > unpack_synidg_msg(
			pair<void*, int> msg_pkg);
	pair<void*, int> pack_synidb_msg(vector<ItemDetail>& item_details);
	pair<ItemDetail*, unsigned int> unpack_synidb_msg(pair<void*, int> msg_pkg);
	pair<void*, int> pack_synrig_msg(vector<RecordInfoType>& record_infos);
	vector<pair<RecordInfoType*, unsigned int> > unpack_synrig_msg(
			pair<void*, int> msg_pkg);
	pair<void*, int> pack_synrib_msg(vector<RecordInfoType>& record_infos);
	pair<RecordInfoType*, unsigned int> unpack_synrib_msg(
			pair<void*, int> msg_pkg);
	pair<void*, int> pack_phis_msg(KItemsets* itemsets);
	KItemsets* unpack_phir_msg(pair<void*, int> msg_pkg);
	pair<void*, int> pack_geng_msg(vector<AssocBaseRule>& assoc_rules);
	vector<AssocBaseRule>* unpack_geng_msg(pair<void*, int> msg_pkg);
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
	ROHashIndex* m_ro_hash_index; //只读哈希索引

private:
	MPI_Comm m_comm;
	int m_root_pid;
//	unsigned int m_global_item_num;
//	char** m_global_identifiers; //全局项目标识
	KItemsets* m_itemset_send_buf;
	KItemsets* m_itemset_recv_buf;
	const static unsigned int SYNIDG_RECV_BUF_SIZE = 409600;
	const static unsigned int SYNIDB_BUF_SIZE = 409600;
	const static unsigned int SYNRIG_RECV_BUF_SIZE = 409600;
	const static unsigned int SYNRIB_BUF_SIZE = 409600;
	const static unsigned int PHIS_BUF_SIZE = 40960;
	const static unsigned int PHIR_BUF_SIZE = 40960;
	const static unsigned int GENG_RECV_BUF_SIZE = 4096000;
	const static unsigned int FRQGENG_RECV_BUF_SIZE = 4096;
	const static unsigned int DEFAULT_OMP_NUM_THREADS = 8;
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::ParallelHiApriori(
		MPI_Comm comm, unsigned int root_pid, unsigned int bucket_size,
		unsigned int global_deep) {
	this->m_item_index = new MPIDHashIndex(comm, bucket_size, global_deep);
	this->m_distributed_index = (DistributedHashIndex*) this->m_item_index;
	this->m_ro_hash_index = NULL;
	this->m_comm = comm;
	this->m_root_pid = root_pid;
	this->m_distributed_index->m_root_pid = root_pid;
//	this->m_global_identifiers = NULL;
//	this->m_global_item_num = 0;
	this->m_itemset_send_buf = NULL;
	this->m_itemset_recv_buf = NULL;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::~ParallelHiApriori() {
//	if (this->m_global_identifiers != NULL && this->m_global_item_num > 0) {
//		for (unsigned int i = 0; i < this->m_global_item_num; i++) {
//			if (this->m_global_identifiers[i] != NULL) {
//				delete[] this->m_global_identifiers[i];
//			}
//		}
//	}
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
//	printf("process %u start syn item detail\n", pid);
	if (!this->syn_item_detail()) {
		return false;
	}
//	printf("process %u finish syn item detail\n", pid);

	//收集记录信息
//	printf("process %u start syn record info\n", pid);
	if (!this->syn_record_info()) {
		return false;
	}
//	printf("process %u finish syn record info\n", pid);

	//同步哈希索引
	if (!this->m_distributed_index->synchronize()) {
		return false;
	}
//	printf("process %u finish syn\n", pid);

	if (!this->m_distributed_index->consolidate()) {
		return false;
	}
//	printf("process %u finish con\n", pid);

	//生成只读索引并传输到加速卡内存
//	printf("start gen ro index\n");
	this->gen_ro_index();
//	printf("end gen ro index\n");

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
		unsigned int result = 0;
//		printf("process %u start get record num of %s\n", pid,
//				local_index.first[i].identifier);
		result = this->m_item_index->get_mark_record_num(
				local_index.first[i].identifier,
				strlen(local_index.first[i].identifier));
//		printf("process %u end get record num is %u\n", pid, result);
		if (result >= this->m_minsup_count) {
			itemset.clear();
//			printf("process %u start get global id of %s\n", pid,
//					local_index.first[i].identifier);
			itemset.push_back(
					get_global_item_id(local_index.first[i].identifier,
							strlen(local_index.first[i].identifier)));
//			printf("process %u end get global id is %u\n", pid,
//					itemset[itemset.size() - 1]);
			frq_itemsets->push(itemset, result);
			this->logItemset("Frequent", 1, itemset, result);
		}
	}
//	if (frq_itemsets->get_itemsets().size() == 0) { //frequent 1-itemsets is not found
//		return false;
//	}
	this->m_frequent_itemsets->push_back(*frq_itemsets);
	delete frq_itemsets;
	delete[] local_index.first; //不要尝试释放结构里面的东西，因为会影响索引结构
//	printf("process %u after f1 gen\n", pid);
//	MPI_Barrier(m_comm);

	/* F2~n generation */
	for (unsigned int i = 0; i + 1 < this->m_max_itemset_size; i++) {
		frq_itemsets = new KItemsets(i + 2,
				1.5 * combine(this->m_item_details.size(), i + 2));

		//复制当前(k-1)频繁项集到接收缓冲区
		this->m_itemset_recv_buf = new KItemsets(
				this->m_frequent_itemsets->at(i));
//		printf("process %u before f%u gen\n", pid, i + 2);
		//F(k-1)[本节点]×F(k-1)[本节点]
		if (!this->phi_frq_gen(*frq_itemsets, this->m_frequent_itemsets->at(i),
				*this->m_itemset_recv_buf)) {
			return false;
		}
//		printf("process %u after f%u gen\n", pid, i + 2);
//		MPI_Barrier(m_comm);

		int buffer_size = PHIS_BUF_SIZE;
		void* buffer = malloc(buffer_size);

		unsigned int comm_times =
				(numprocs % 2 == 0) ? numprocs / 2 - 1 : numprocs / 2;
		unsigned int j = 0;
		for (j = 0; j < comm_times; j++) {
			this->m_itemset_send_buf = new KItemsets();
			this->toggle_buffer();
//			printf("process %u f%u start %u time pack\n", pid, i + 2, j);
			//准备接收数据缓冲区
			pair<void*, int> phir_msg_pkg;
			phir_msg_pkg.first = malloc(PHIR_BUF_SIZE);
			phir_msg_pkg.second = PHIR_BUF_SIZE;

			//用发送缓冲区的数据打包发送消息
			pair<void*, int> phis_msg_pkg = this->pack_phis_msg(
					this->m_itemset_send_buf);

//			printf("process %u before f%u %u time bsend\n", pid, i + 2, j);
			//以非阻塞方式数据发送给下一个节点
			MPI_Buffer_attach(buffer, PHIS_BUF_SIZE);
			MPI_Bsend(phis_msg_pkg.first, phis_msg_pkg.second, MPI_PACKED,
					(pid + 1) % numprocs, j, m_comm);

//			printf("process %u before f%u %u time recv\n", pid, i + 2, j);
			//以阻塞方式从上一节点接收数据
			MPI_Status comm_status;
			MPI_Recv(phir_msg_pkg.first, phir_msg_pkg.second, MPI_PACKED,
					(pid + numprocs - 1) % numprocs, j, m_comm, &comm_status);

			//把接收数据解包到接收缓冲区
			this->m_itemset_recv_buf = unpack_phir_msg(phir_msg_pkg);

			//F(k-1)[本节点]×F(k-1)[上j+1节点]
			if (!this->phi_frq_gen(*frq_itemsets,
					this->m_frequent_itemsets->at(i),
					*this->m_itemset_recv_buf)) {
				return false;
			}

//			printf("process %u after f%u %u time recv\n", pid, i + 2, j);
			//栅栏同步
			MPI_Barrier(m_comm);

			delete this->m_itemset_send_buf;
			MPI_Buffer_detach(&buffer, &buffer_size);
		}

		if (numprocs % 2 == 0) {
			if (numprocs % 4 != 0) {
				//这种情况可以所有结点同时进行通讯，每个结点只参与发送/接收的一种，理论上最快的方式
				if (pid % 2 == 0) { //进程号为偶数的发送
					this->toggle_buffer();
					//用发送缓冲区的数据打包发送消息
					pair<void*, int> phis_msg_pkg = this->pack_phis_msg(
							this->m_itemset_send_buf);
					//以非阻塞方式数据发送给下一个节点
					MPI_Buffer_attach(buffer, PHIS_BUF_SIZE);
					MPI_Bsend(phis_msg_pkg.first, phis_msg_pkg.second,
							MPI_PACKED, (pid + 1) % numprocs, j, m_comm);
					//栅栏同步
//					MPI_Barrier(m_comm);
					this->toggle_buffer();
					MPI_Buffer_detach(&buffer, &buffer_size);
				} else { //进程号为奇数的接收
					delete this->m_itemset_recv_buf;
					//准备接收数据缓冲区
					pair<void*, int> phir_msg_pkg;
					phir_msg_pkg.first = malloc(PHIR_BUF_SIZE);
					phir_msg_pkg.second = PHIR_BUF_SIZE;

					//以阻塞方式从上一节点接收数据
					MPI_Status comm_status;
					MPI_Recv(phir_msg_pkg.first, phir_msg_pkg.second,
							MPI_PACKED, (pid + numprocs - 1) % numprocs, j,
							m_comm, &comm_status);

					//把接收数据解包到接收缓冲区
					this->m_itemset_recv_buf = unpack_phir_msg(phir_msg_pkg);

					//F(k-1)[本节点]×F(k-1)[上j+1节点]
					if (!this->phi_frq_gen(*frq_itemsets,
							this->m_frequent_itemsets->at(i),
							*this->m_itemset_recv_buf)) {
						return false;
					}

					//栅栏同步
//					MPI_Barrier(m_comm);
				}
				//栅栏同步
				MPI_Barrier(m_comm);
			} else {
				//这种情况只有n/2+1个节点进行通讯，每个结点参与发送/接收的一种或两种，理论上中等速度的方式
				this->m_itemset_send_buf = new KItemsets();
				this->toggle_buffer();

				if (pid < numprocs / 2) {
					//用发送缓冲区的数据打包发送消息
					pair<void*, int> phis_msg_pkg = this->pack_phis_msg(
							this->m_itemset_send_buf);

					//以非阻塞方式数据发送给下一个节点
					MPI_Buffer_attach(buffer, PHIS_BUF_SIZE);
					MPI_Bsend(phis_msg_pkg.first, phis_msg_pkg.second,
							MPI_PACKED, (pid + 1) % numprocs, j, m_comm);
					MPI_Buffer_detach(&buffer, &buffer_size);
				}

				if (pid > 0 && pid <= numprocs / 2) {
					//准备接收数据缓冲区
					pair<void*, int> phir_msg_pkg;
					phir_msg_pkg.first = malloc(PHIR_BUF_SIZE);
					phir_msg_pkg.second = PHIR_BUF_SIZE;

					//以阻塞方式从上一节点接收数据
					MPI_Status comm_status;
					MPI_Recv(phir_msg_pkg.first, phir_msg_pkg.second,
							MPI_PACKED, (pid + numprocs - 1) % numprocs, j,
							m_comm, &comm_status);

					//把接收数据解包到接收缓冲区
					this->m_itemset_recv_buf = unpack_phir_msg(phir_msg_pkg);

					//F(k-1)[本节点]×F(k-1)[上j+1节点]
					if (!this->phi_frq_gen(*frq_itemsets,
							this->m_frequent_itemsets->at(i),
							*this->m_itemset_recv_buf)) {
						return false;
					}
				}

				//栅栏同步
				MPI_Barrier(m_comm);
				delete this->m_itemset_send_buf;
			}
//			printf("process %u have %u f%u itemsets after last round comm\n",
//					pid, frq_itemsets->get_itemsets().size(), i + 2);
		}

		delete this->m_itemset_recv_buf;
		free(buffer);

		//如果有新增的频繁项集则加入，如果所有进程都没有新的频繁项集则证明没有必要继续
		if (frq_itemsets->get_itemsets().size() > 0) {
			this->m_frequent_itemsets->push_back(*frq_itemsets);
		}

		unsigned int max_frq_itemsets_num = 0;
		unsigned int frq_itemsets_num = this->m_frequent_itemsets->size();
		MPI_Reduce(&frq_itemsets_num, &max_frq_itemsets_num, 1, MPI_UNSIGNED,
				MPI_MAX, m_root_pid, m_comm);
		MPI_Bcast(&max_frq_itemsets_num, 1, MPI_UNSIGNED, m_root_pid, m_comm);
//		printf(
//				"process %u frq-%u-itemsets size is:%u, frq_itemsets_num:%u, max_frq_itemsets_num:%u\n",
//				pid, i + 2, frq_itemsets->get_itemsets().size(),
//				this->m_frequent_itemsets->size(), max_frq_itemsets_num);
		if (max_frq_itemsets_num != i + 2) {
			break;
		} else if (this->m_frequent_itemsets->size() < max_frq_itemsets_num) {
			//把空的部分频繁项集也加进来
			this->m_frequent_itemsets->push_back(*frq_itemsets);
		}

		//栅栏同步
		MPI_Barrier(m_comm);
		delete frq_itemsets;
	}
//	printf("process %u finish gen frequent itemset\n", pid);
//	MPI_Barrier(m_comm);

	//释放只读索引所占的内存空间
//	printf("start destroy ro index\n");
	this->destroy_ro_index();
//	printf("end destroy ro index\n");

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_frq_gen(
		KItemsets& frq_itemset, KItemsets& prv_frq1, KItemsets& prv_frq2) {
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets1 =
			prv_frq1.get_itemsets();
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets2 =
			prv_frq2.get_itemsets();

	if (prv_frq_itemsets1.size() == 0 || prv_frq_itemsets2.size() == 0) {
		return true;
	}

	//把输入数据转化成矩阵以便在并行区域随机读取
	unsigned int** frq_itemset_list1 =
			new unsigned int*[prv_frq_itemsets1.size()];
	for (unsigned int i = 0; i < prv_frq_itemsets1.size(); i++) {
		frq_itemset_list1[i] = new unsigned int[prv_frq1.get_term_num()];
	}
	unsigned int** frq_itemset_list2 =
			new unsigned int*[prv_frq_itemsets2.size()];
	for (unsigned int i = 0; i < prv_frq_itemsets2.size(); i++) {
		frq_itemset_list2[i] = new unsigned int[prv_frq2.get_term_num()];
	}
	unsigned int i = 0;
	for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter1 =
			prv_frq_itemsets1.begin(); prv_frq_iter1 != prv_frq_itemsets1.end();
			prv_frq_iter1++, i++) {
//		memcpy(frq_itemset_list1 + i, prv_frq_iter1->first.data(),
//				prv_frq1.get_term_num());
		for (unsigned int j = 0; j < prv_frq1.get_term_num(); j++) {
			frq_itemset_list1[i][j] = prv_frq_iter1->first.data()[j];
		}
	}
	i = 0;
	for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter2 =
			prv_frq_itemsets2.begin(); prv_frq_iter2 != prv_frq_itemsets2.end();
			prv_frq_iter2++, i++) {
//		memcpy(frq_itemset_list2 + i, prv_frq_iter2->first.data(),
//				prv_frq2.get_term_num());
		for (unsigned int j = 0; j < prv_frq2.get_term_num(); j++) {
			frq_itemset_list2[i][j] = prv_frq_iter2->first.data()[j];
		}
	}

	/*使用OpenMP多线程并行化*/
	//设置线程数
	char* omp_num_threads = getenv("OMP_NUM_THREADS");
	unsigned int numthreads =
			omp_num_threads != NULL ?
					atoi(omp_num_threads) : DEFAULT_OMP_NUM_THREADS;
	//初始化归并结果缓冲区
	unsigned int* frq_itemset_buf = new unsigned int[numthreads
			* FRQGENG_RECV_BUF_SIZE];
	unsigned int* frq_itemset_num = new unsigned int[numthreads];
	memset(frq_itemset_buf, 0,
			numthreads * FRQGENG_RECV_BUF_SIZE * sizeof(unsigned int));
	memset(frq_itemset_num, 0, numthreads * sizeof(unsigned int));

	/* 潜在频繁项集生成 */
#pragma omp parallel for num_threads(numthreads) shared(frq_itemset_buf, frq_itemset_num)
	for (unsigned int k = 0;
			k < prv_frq_itemsets1.size() * prv_frq_itemsets2.size(); k++) {
		unsigned int tid = omp_get_thread_num();
//		printf("thread:%u is running\n", tid);
		unsigned int i = k / prv_frq_itemsets2.size();
		unsigned int j = k % prv_frq_itemsets2.size();
		vector<unsigned int> prv_frq_itemset1(frq_itemset_list1[i],
				frq_itemset_list1[i] + prv_frq1.get_term_num());
		vector<unsigned int> prv_frq_itemset2(frq_itemset_list2[j],
				frq_itemset_list2[j] + prv_frq2.get_term_num());

		/************************** 项目集连接与过滤 **************************/
		//求并集
//		printf("start calc union\n");
		vector<unsigned int>* k_itemset = NULL;
		if (prv_frq_itemset1.size() == 1 || prv_frq_itemset2.size() == 1) { //F(k-1)×F(1)
			k_itemset = KItemsets::union_set(prv_frq_itemset1,
					prv_frq_itemset2);
		} else if (prv_frq_itemset1.size() == prv_frq_itemset2.size()) { //F(k-1)×F(k-1)
			k_itemset = KItemsets::union_eq_set(prv_frq_itemset1,
					prv_frq_itemset2);
		}
//		printf("end calc union\n");

		//过滤并保存潜在频繁项集
		unsigned int support;
		if (k_itemset != NULL && k_itemset->size() == frq_itemset.get_term_num()
				&& this->phi_filter(k_itemset, &support)) {
//			printf("start write result\n");
			unsigned int* offset = frq_itemset_buf + tid * FRQGENG_RECV_BUF_SIZE
					+ frq_itemset_num[tid] * (frq_itemset.get_term_num() + 1);
			memcpy(offset, k_itemset->data(),
					k_itemset->size() * sizeof(unsigned int));
			memcpy(offset + frq_itemset.get_term_num(), &support,
					1 * sizeof(unsigned int));
			frq_itemset_num[tid]++;
//			printf("end write result\n");
		}
		delete k_itemset;
	}

//	printf("start gather result\n");
	//合并结果
	for (unsigned int i = 0; i < numthreads; i++) {
		for (unsigned int j = 0; j < frq_itemset_num[i]; j++) {
			unsigned int* offset = frq_itemset_buf + i * FRQGENG_RECV_BUF_SIZE
					+ j * (frq_itemset.get_term_num() + 1);
			vector<unsigned int> itemset(offset,
					offset + frq_itemset.get_term_num());
			unsigned int support = 0;
			memcpy(&support, offset + frq_itemset.get_term_num(),
					1 * sizeof(unsigned int));
			frq_itemset.push(itemset, support);
			this->logItemset("Frequent", itemset.size(), itemset, support);
		}
	}
//	printf("end gather result\n");
	for (unsigned int i = 0; i < prv_frq_itemsets1.size(); i++) {
		delete[] frq_itemset_list1[i];
	}
	delete[] frq_itemset_list1;
	for (unsigned int i = 0; i < prv_frq_itemsets2.size(); i++) {
		delete[] frq_itemset_list2[i];
	}
	delete[] frq_itemset_list2;
	delete[] frq_itemset_buf;
	delete[] frq_itemset_num;

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_filter(
		vector<unsigned int>* k_itemset, unsigned int* support) {
	char **keys = new char*[k_itemset->size()];
	if (0 == this->m_minsup_count)
		this->m_minsup_count = 1;
	unsigned int *result;
	for (unsigned int j = 0; j < k_itemset->size(); j++) {
		keys[j] = this->m_item_details[k_itemset->at(j)].m_identifier;
	}
	result = this->m_ro_hash_index->get_intersect_records((const char **) keys,
			k_itemset->size());
	*support = result[0];
	delete[] keys;
	delete[] result;

	return *support >= this->m_minsup_count;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_genrules() {
	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);

//	printf("process %u start gen local rules\n", pid);
//	MPI_Barrier(m_comm);
	bool gen_succeed = this->genrules();
//	printf("process %u end gen local rules\n", pid);
//	MPI_Barrier(m_comm);
	if (!gen_succeed)
		return false;

	if (numprocs == 1)
		return true;

	//打包Gather消息
//	printf("process %u start pack g rules\n", pid);
	pair<void*, int> geng_send_msg_pkg = pack_geng_msg(
			this->m_assoc_base_rules);
//	printf("process %u end pack g rules\n", pid);

	//准备Gather消息缓冲区
	pair<void*, int> geng_recv_msg_pkg;
	geng_recv_msg_pkg.first = NULL;
	geng_recv_msg_pkg.second = 0;
	if (pid == m_root_pid) {
		geng_recv_msg_pkg.first = malloc(numprocs * GENG_RECV_BUF_SIZE);
		geng_recv_msg_pkg.second = GENG_RECV_BUF_SIZE;
	}

//	printf("process %u start gather rules\n", pid);
	MPI_Gather(geng_send_msg_pkg.first, geng_send_msg_pkg.second, MPI_PACKED,
			geng_recv_msg_pkg.first, GENG_RECV_BUF_SIZE, MPI_PACKED, m_root_pid,
			m_comm);
//	printf("process %u end gather rules\n", pid);

	//处理汇总数据
	if (pid == m_root_pid) {
		//解包Gather消息
//		printf("process %u start unpack gather rules\n", pid);
		vector<AssocBaseRule>* geng_recv_msg = unpack_geng_msg(
				geng_recv_msg_pkg);
//		printf("process %u end unpack gather rules\n", pid);
		for (unsigned int i = 0; i < geng_recv_msg->size(); i++) {
			this->m_assoc_base_rules.push_back(geng_recv_msg->at(i));
		}

		delete geng_recv_msg;

		//把关联规则展开
//		printf("process %u start unfold gather rules\n", pid);
		this->m_assoc_rules->clear();
		for (unsigned int i = 0; i < this->m_assoc_base_rules.size(); i++) {
			AssociationRule<ItemDetail> assoc_rule;
			for (unsigned int j = 0;
					j < this->m_assoc_base_rules[i].condition.size(); j++) {
				assoc_rule.condition.push_back(
						this->m_item_details[this->m_assoc_base_rules[i].condition[j]]);
			}
			for (unsigned int j = 0;
					j < this->m_assoc_base_rules[i].consequent.size(); j++) {
				assoc_rule.consequent.push_back(
						this->m_item_details[this->m_assoc_base_rules[i].consequent[j]]);
			}
			assoc_rule.confidence = this->m_assoc_base_rules[i].confidence;
			this->m_assoc_rules->push_back(assoc_rule);
		}
//		printf("process %u end unfold gather rules\n", pid);
	}

	free(geng_send_msg_pkg.first);
	if (pid == m_root_pid) {
		free(geng_recv_msg_pkg.first);
	}

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::gen_ro_index() {
	if (this->m_ro_hash_index != NULL) {
		delete this->m_ro_hash_index;
	}
	this->m_ro_hash_index = new RODynamicHashIndex();
	bool succeed = true;
	succeed &= this->m_ro_hash_index->build(this->m_item_index);
	return succeed;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::destroy_ro_index() {
	bool succeed = true;
	if (this->m_ro_hash_index != NULL) {
		delete this->m_ro_hash_index;
	}
	return succeed;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::syn_item_detail() {
	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);

	//准备Gather接收的数据缓冲区
	pair<void*, int> synidg_recv_msg_pkg;
	synidg_recv_msg_pkg.first = NULL;
	synidg_recv_msg_pkg.second = 0;
	if (pid == m_root_pid) {
		synidg_recv_msg_pkg.second = numprocs * SYNIDG_RECV_BUF_SIZE;
		synidg_recv_msg_pkg.first = malloc(synidg_recv_msg_pkg.second);
	}

	//打包Gather数据
	pair<void*, int> synidg_send_msg_pkg = pack_synidg_msg(
			this->m_item_details);

//	printf("process %u start gather\n", pid);
	MPI_Gather(synidg_send_msg_pkg.first, synidg_send_msg_pkg.second,
			MPI_PACKED, synidg_recv_msg_pkg.first, SYNIDG_RECV_BUF_SIZE,
			MPI_PACKED, m_root_pid, m_comm);
//	printf("process %u end gather\n", pid);

	//声明Broadcast数据包
	pair<void*, int> synidb_msg_pkg;

	if (pid == m_root_pid) {
		//解包Gather数据
//		printf("start unpack phig\n");
		vector<pair<ItemDetail*, unsigned int> > synidg_recv_msg =
				unpack_synidg_msg(synidg_recv_msg_pkg);
//		printf("end unpack phig\n");
		assert(synidg_recv_msg.size() == numprocs);

		//清空原有项目详情
		this->m_item_details.clear();

		//处理Gather数据和准备Broadcast数据
		HashIndex* index = new OpenHashIndex(
				2.5 * synidg_recv_msg.size() * synidg_recv_msg[0].second);
		for (unsigned int i = 0; i < synidg_recv_msg.size(); i++) {
			for (unsigned int j = 0; j < synidg_recv_msg[i].second; j++) {
				char* key_info = NULL;
				index->get_key_info(&key_info,
						synidg_recv_msg[i].first[j].m_identifier,
						strlen(synidg_recv_msg[i].first[j].m_identifier));
				if (key_info == NULL) {
					this->m_item_details.push_back(synidg_recv_msg[i].first[j]); //加入新数据
					index->insert(synidg_recv_msg[i].first[j].m_identifier,
							strlen(synidg_recv_msg[i].first[j].m_identifier),
							"null", 0);
				} else {
					delete[] key_info;
				}
			}
			delete[] synidg_recv_msg[i].first; //清除接收数据
		}
		delete index;

		//打包Broadcast数据
		synidb_msg_pkg = pack_synidb_msg(this->m_item_details);
	} else {
		vector<ItemDetail> null_item_detail;
		synidb_msg_pkg = pack_synidb_msg(null_item_detail);
	}

//	printf("process %u start bcast\n", pid);
//	MPI_Barrier(m_comm);
	MPI_Bcast(synidb_msg_pkg.first, synidb_msg_pkg.second, MPI_PACKED,
			m_root_pid, m_comm);
//	printf("process %u end bcast\n", pid);

	//解包Broadcast数据
//	printf("\n\nProcess %i Start unpack b\n", pid);
	pair<ItemDetail*, unsigned int> synidb_msg = unpack_synidb_msg(
			synidb_msg_pkg);
//	printf("\n\nProcess %i Finish unpack b\n", pid);

	//处理Broadcast数据
	if (pid != m_root_pid) {
		//更新项目详情
		this->m_item_details.clear();
		for (unsigned int i = 0; i < synidb_msg.second; i++) {
			this->m_item_details.push_back(synidb_msg.first[i]);
		}
	}

	//更新索引的key_info为全局信息
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		char* key_info = NULL;
		this->m_item_index->get_key_info(&key_info,
				this->m_item_details[i].m_identifier,
				strlen(this->m_item_details[i].m_identifier));
		if (key_info != NULL) {
			char* gid_str = itoa(i, 10);
			char* new_key_info =
					new char[strlen(key_info) + strlen(gid_str) + 2];
			strcpy(new_key_info, key_info);
			strcat(new_key_info, "#");
			strcat(new_key_info, gid_str);
			this->m_item_index->change_key_info(
					this->m_item_details[i].m_identifier,
					strlen(this->m_item_details[i].m_identifier), new_key_info);
			delete[] gid_str;
			delete[] new_key_info;
		}
	}

	//栅栏同步
	MPI_Barrier(m_comm);

	free(synidg_send_msg_pkg.first);
	free(synidg_recv_msg_pkg.first);

	delete[] synidb_msg.first;
	free(synidb_msg_pkg.first);

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::syn_record_info() {
	int pid, numprocs;
	MPI_Comm_rank(m_comm, &pid);
	MPI_Comm_size(m_comm, &numprocs);

	//准备Gather接收的数据缓冲区
	pair<void*, int> synrig_recv_msg_pkg;
	synrig_recv_msg_pkg.first = NULL;
	synrig_recv_msg_pkg.second = 0;
	if (pid == m_root_pid) {
		synrig_recv_msg_pkg.second = numprocs * SYNRIG_RECV_BUF_SIZE;
		synrig_recv_msg_pkg.first = malloc(synrig_recv_msg_pkg.second);
	}

	//打包Gather数据
//	printf("process %u start pack syn record info\n", pid);
	pair<void*, int> synrig_send_msg_pkg = pack_synrig_msg(
			this->m_record_infos);
//	printf("process %u end pack syn record info\n", pid);

	MPI_Gather(synrig_send_msg_pkg.first, synrig_send_msg_pkg.second,
			MPI_PACKED, synrig_recv_msg_pkg.first, SYNRIG_RECV_BUF_SIZE,
			MPI_PACKED, m_root_pid, m_comm);

	//声明Broadcast数据包
	pair<void*, int> synrib_msg_pkg;

	if (pid == m_root_pid) {
		//解包Gather数据
		vector<pair<RecordInfoType*, unsigned int> > synrig_recv_msg =
				unpack_synrig_msg(synrig_recv_msg_pkg);
		assert(synrig_recv_msg.size() == numprocs);

		//清空原有项目详情
		this->m_record_infos.clear();

		//处理Gather数据和准备Broadcast数据
		for (unsigned int i = 0; i < synrig_recv_msg.size(); i++) {
			for (unsigned int j = 0; j < synrig_recv_msg[i].second; j++) {
				this->m_record_infos.push_back(synrig_recv_msg[i].first[j]); //加入新数据
			}
			delete[] synrig_recv_msg[i].first; //清除接收数据
		}

		//打包Broadcast数据
		synrib_msg_pkg = pack_synrib_msg(this->m_record_infos);
	} else {
		vector<RecordInfoType> null_record_infos;
		synrib_msg_pkg = pack_synrib_msg(null_record_infos);
	}

	MPI_Bcast(synrib_msg_pkg.first, synrib_msg_pkg.second, MPI_PACKED,
			m_root_pid, m_comm);

	//解包Broadcast数据
	pair<RecordInfoType*, unsigned int> synrib_msg = unpack_synrib_msg(
			synrib_msg_pkg);

	//处理Broadcast数据
	if (pid != m_root_pid) {
		//更新项目详情
		this->m_record_infos.clear();
		for (unsigned int i = 0; i < synrib_msg.second; i++) {
			this->m_record_infos.push_back(synrib_msg.first[i]);
		}
	}

	//栅栏同步
	MPI_Barrier(m_comm);

	free(synrig_send_msg_pkg.first);
	free(synrig_recv_msg_pkg.first);

	delete[] synrib_msg.first;
	free(synrib_msg_pkg.first);

	return true;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_synidg_msg(
		vector<ItemDetail>& item_detail) {
	pair<void*, int> result;

	//计算缓冲区大小：
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &result.second);
	for (unsigned int i = 0; i < item_detail.size(); i++) {
		result.second += item_detail[i].get_mpi_pack_size(m_comm);
	}
	assert(result.second <= SYNIDG_RECV_BUF_SIZE);
	result.second = SYNIDG_RECV_BUF_SIZE;

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
		RecordInfoType>::unpack_synidg_msg(pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	MPI_Comm_size(m_comm, &numprocs);
	vector<pair<ItemDetail*, unsigned int> > result;
	for (unsigned int i = 0; i < numprocs; i++) {
		pair<ItemDetail*, unsigned int> result_item;
		position = i * SYNIDG_RECV_BUF_SIZE;
		unsigned int item_num = 0;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &item_num, 1,
				MPI_UNSIGNED, m_comm);
		result_item.second = item_num;
		result_item.first = new ItemDetail[item_num];
//		printf("start unpack element:%u\n", item_num);
		ItemDetail::mpi_unpack(msg_pkg.first, msg_pkg.second, &position,
				result_item.first, item_num, m_comm);
//		printf("end unpack element\n");
		result.push_back(result_item);
	}
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_synidb_msg(
		vector<ItemDetail>& item_details) {
	pair<void*, int> result;

	//计算缓冲区大小：
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &result.second);
	for (unsigned int i = 0; i < item_details.size(); i++) {
		result.second += item_details[i].get_mpi_pack_size(m_comm);
	}
	assert(result.second <= SYNIDB_BUF_SIZE);
	result.second = SYNIDB_BUF_SIZE;

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	if (item_details.size() > 0) {
		int position = 0;
		unsigned int item_num = item_details.size();
		MPI_Pack(&item_num, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		for (unsigned int i = 0; i < item_num; i++) {
			pair<void*, int> item_pkg = item_details[i].mpi_pack(m_comm);
			MPI_Pack(item_pkg.first, item_pkg.second, MPI_PACKED, result.first,
					result.second, &position, m_comm);
			free(item_pkg.first);
		}
	}

	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<ItemDetail*, unsigned int> ParallelHiApriori<ItemType, ItemDetail,
		RecordInfoType>::unpack_synidb_msg(pair<void*, int> msg_pkg) {
	int position = 0;
	pair<ItemDetail*, unsigned int> result;
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &result.second, 1,
			MPI_UNSIGNED, m_comm);
	result.first = new ItemDetail[result.second];
	ItemDetail::mpi_unpack(msg_pkg.first, msg_pkg.second, &position,
			result.first, result.second, m_comm);
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_synrig_msg(
		vector<RecordInfoType>& record_infos) {
	pair<void*, int> result;

	//计算缓冲区大小：
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &result.second);
	for (unsigned int i = 0; i < record_infos.size(); i++) {
		result.second += record_infos[i].get_mpi_pack_size(m_comm);
	}
	assert(result.second <= SYNRIG_RECV_BUF_SIZE);
	result.second = SYNRIG_RECV_BUF_SIZE;

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	int position = 0;
	unsigned int record_num = record_infos.size();
	MPI_Pack(&record_num, 1, MPI_UNSIGNED, result.first, result.second,
			&position, m_comm);
	for (unsigned int i = 0; i < record_num; i++) {
		pair<void*, int> record_pkg = record_infos[i].mpi_pack(m_comm);
		MPI_Pack(record_pkg.first, record_pkg.second, MPI_PACKED, result.first,
				result.second, &position, m_comm);
		free(record_pkg.first);
	}

	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
vector<pair<RecordInfoType*, unsigned int> > ParallelHiApriori<ItemType,
		ItemDetail, RecordInfoType>::unpack_synrig_msg(
		pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	MPI_Comm_size(m_comm, &numprocs);

	vector<pair<RecordInfoType*, unsigned int> > result;
	for (unsigned int i = 0; i < numprocs; i++) {
		pair<RecordInfoType*, unsigned int> result_item;
		position = i * SYNRIG_RECV_BUF_SIZE;
		unsigned int record_num = 0;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &record_num, 1,
				MPI_UNSIGNED, m_comm);
		result_item.second = record_num;
		result_item.first = new RecordInfoType[record_num];
		RecordInfoType::mpi_unpack(msg_pkg.first, msg_pkg.second, &position,
				result_item.first, record_num, m_comm);
		result.push_back(result_item);
	}
	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<void*, int> ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::pack_synrib_msg(
		vector<RecordInfoType>& record_infos) {
	pair<void*, int> result;

	//计算缓冲区大小：
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &result.second);
	for (unsigned int i = 0; i < record_infos.size(); i++) {
		result.second += record_infos[i].get_mpi_pack_size(m_comm);
	}
	assert(result.second <= SYNRIB_BUF_SIZE);
	result.second = SYNRIB_BUF_SIZE;

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	if (record_infos.size() > 0) {
		int position = 0;
		unsigned int record_num = record_infos.size();
		MPI_Pack(&record_num, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		for (unsigned int i = 0; i < record_num; i++) {
			pair<void*, int> record_pkg = record_infos[i].mpi_pack(m_comm);
			MPI_Pack(record_pkg.first, record_pkg.second, MPI_PACKED,
					result.first, result.second, &position, m_comm);
			free(record_pkg.first);
		}
	}

	return result;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
pair<RecordInfoType*, unsigned int> ParallelHiApriori<ItemType, ItemDetail,
		RecordInfoType>::unpack_synrib_msg(pair<void*, int> msg_pkg) {
	int position = 0;
	pair<RecordInfoType*, unsigned int> result;
	MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &result.second, 1,
			MPI_UNSIGNED, m_comm);
	result.first = new RecordInfoType[result.second];
	RecordInfoType::mpi_unpack(msg_pkg.first, msg_pkg.second, &position,
			result.first, result.second, m_comm);
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
		vector<AssocBaseRule>& assoc_rules) {
	pair<void*, int> result;

	//计算缓冲区大小：
	int assoc_num_mpi_size, conf_mpi_size, item_index_num_mpi_size,
			item_indices_mpi_size = 0;
	int conf_num = assoc_rules.size();
	int item_index_num = 2 * assoc_rules.size();
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &assoc_num_mpi_size);
	MPI_Pack_size(conf_num, MPI_DOUBLE, m_comm, &conf_mpi_size);
	MPI_Pack_size(item_index_num, MPI_UNSIGNED, m_comm,
			&item_index_num_mpi_size);
	int item_index_mpi_size;
	MPI_Pack_size(1, MPI_UNSIGNED, m_comm, &item_index_mpi_size);
	for (unsigned int i = 0; i < assoc_rules.size(); i++) {
		item_indices_mpi_size += assoc_rules[i].condition.size()
				* item_index_mpi_size;

		item_indices_mpi_size += assoc_rules[i].consequent.size()
				* item_index_mpi_size;
	}
	result.second = assoc_num_mpi_size + conf_mpi_size + item_index_num_mpi_size
			+ item_indices_mpi_size; //规则数量，每条规则的信任度，每条规则的条件和结果的数量，每条规则的条件和结果
	assert(result.second <= GENG_RECV_BUF_SIZE);
	result.second = GENG_RECV_BUF_SIZE;

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	int position = 0;
	unsigned int rule_num = assoc_rules.size();
	MPI_Pack(&rule_num, 1, MPI_UNSIGNED, result.first, result.second, &position,
			m_comm);
	for (unsigned int i = 0; i < assoc_rules.size(); i++) {
		unsigned int cond_num = assoc_rules[i].condition.size();
		MPI_Pack(&cond_num, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		MPI_Pack(assoc_rules[i].condition.data(),
				assoc_rules[i].condition.size(), MPI_UNSIGNED, result.first,
				result.second, &position, m_comm);

		unsigned int cons_num = assoc_rules[i].consequent.size();
		MPI_Pack(&cons_num, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		MPI_Pack(assoc_rules[i].consequent.data(),
				assoc_rules[i].consequent.size(), MPI_UNSIGNED, result.first,
				result.second, &position, m_comm);
		double confidence = assoc_rules[i].confidence;
		MPI_Pack(&confidence, 1, MPI_DOUBLE, result.first, result.second,
				&position, m_comm);
	}

	return result;
}
template<typename ItemType, typename ItemDetail, typename RecordInfoType>
vector<AssocBaseRule>* ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::unpack_geng_msg(
		pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	MPI_Comm_size(m_comm, &numprocs);

	unsigned int rule_num, cond_num, cons_num;
	vector<AssocBaseRule>* result = new vector<AssocBaseRule>();
	for (unsigned int i = 0; i < numprocs; i++) {
		position = i * GENG_RECV_BUF_SIZE;
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &rule_num, 1,
				MPI_UNSIGNED, m_comm);
		for (unsigned int j = 0; j < rule_num; j++) {
			AssocBaseRule assoc_rule;
			MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &cond_num, 1,
					MPI_UNSIGNED, m_comm);
			unsigned int condition[cond_num];
			MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, condition,
					cond_num, MPI_UNSIGNED, m_comm);
			for (unsigned int k = 0; k < cond_num; k++) {
				assoc_rule.condition.push_back(condition[k]);
			}

			MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, &cons_num, 1,
					MPI_UNSIGNED, m_comm);
			unsigned int consequent[cons_num];
			MPI_Unpack(msg_pkg.first, msg_pkg.second, &position, consequent,
					cons_num, MPI_UNSIGNED, m_comm);
			for (unsigned int k = 0; k < cons_num; k++) {
				assoc_rule.consequent.push_back(consequent[k]);
			}

			MPI_Unpack(msg_pkg.first, msg_pkg.second, &position,
					&assoc_rule.confidence, 1, MPI_DOUBLE, m_comm);

			result->push_back(assoc_rule);
		}
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
//	printf("split %s\n", key_info);
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
