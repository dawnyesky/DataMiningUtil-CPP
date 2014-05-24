/*
 * phi_apriori.h
 *
 *  Created on: 2013-8-5
 *      Author: Yan Shankai
 */

#ifndef PHI_APRIORI_H_
#define PHI_APRIORI_H_

#ifdef OMP
#include <omp.h>
#include <offload.h>
#include "libyskdmu/macro.h"
#include "libyskdmu/util/mic_util.h"
#elif defined(OCL)
#define OPENCL_CHECK_ERRORS(ERR)					\
		if(ERR != CL_SUCCESS) {						\
			cerr									\
			<< "OpenCL error with code " << ERR		\
			<< " happened in file " << __FILE__		\
			<< " at line " << __LINE__				\
			<< ". Exiting...\n";					\
			exit(1);								\
		}

#define OPENCL_GET_NUMERIC_PROPERTY(DEVICE, NAME, VALUE) {		\
            size_t property_length = 0;							\
            err = clGetDeviceInfo(								\
            		DEVICE,										\
            		NAME,										\
            		sizeof(VALUE),								\
            		&VALUE,										\
            		&property_length							\
            );													\
            assert(property_length == sizeof(VALUE));			\
            OPENCL_CHECK_ERRORS(err);							\
		}

#include <CL/cl.h>
#include <unistd.h>
#endif

#include <math.h>
#include <string.h>
#include <numeric>
#include <limits>
#include "libyskalgrthms/util/string.h"
#include "libyskdmu/index/distributed_hash_index.h"
#include "libyskdmu/index/mpi_d_hash_index.h"
#include "libyskdmu/index/ro_dhi_data.h"
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
#ifdef OMP
	bool phi_filter(vector<unsigned int>* k_itemset, unsigned int* support,
			ROHashIndex* ro_index, char* identifiers,
			unsigned int identifiers_size, unsigned int* id_index,
			unsigned int id_index_size, unsigned int minsup_count);
#elif defined(OCL)
	bool phi_filter(vector<vector<unsigned long long> >* intersect_result,
			vector<unsigned long long>* data,
			vector<unsigned long long>* in_indice,
			vector<unsigned long long>* in_offset,
			vector<unsigned long long>* out_indice,
			vector<unsigned long long>* out_offset,
			vector<unsigned long long>* out_offset_indice,
			vector<unsigned long long>* task_indice, size_t total_work_load,
			size_t total_work_task);
	bool init_opencl();
	bool release_opencl();
#else
	bool phi_filter(vector<unsigned int>* k_itemset, unsigned int* support);
#endif
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
	bool gen_identifiers(char** identifiers, unsigned int* identifiers_size,
			unsigned int** id_index, unsigned int* id_index_size);
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
	ROHashIndexData* m_ro_hi_data; //只读哈希索引

private:
	MPI_Comm m_comm;
	int m_root_pid;
	unsigned int m_device_id;
	pair<char*, unsigned int> m_identifiers; //全局项目标识
	pair<unsigned int*, unsigned int> m_id_index; //全局项目标识的索引
	KItemsets* m_itemset_send_buf;
	KItemsets* m_itemset_recv_buf;
	unsigned int SYNIDG_RECV_BUF_SIZE;
	unsigned int SYNIDB_BUF_SIZE;
	unsigned int SYNRIG_RECV_BUF_SIZE;
	unsigned int SYNRIB_BUF_SIZE;
	unsigned int PHIS_BUF_SIZE;
	unsigned int PHIR_BUF_SIZE;
	unsigned int GENG_RECV_BUF_SIZE;
	unsigned int FRQGENG_RECV_BUF_SIZE;
#ifdef OMP
	unsigned int DEFAULT_OMP_NUM_THREADS;
#elif defined(OCL)
	vector<cl_device_id> m_cl_devices;
	vector<cl_device_type> m_cl_device_types;
	vector<float> m_cl_device_perfs;
	vector<cl_context> m_cl_contexts;
#endif
};

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::ParallelHiApriori(
		MPI_Comm comm, unsigned int root_pid, unsigned int bucket_size,
		unsigned int global_deep) {
	this->m_item_index = new MPIDHashIndex(comm, bucket_size, global_deep);
	this->m_distributed_index = (DistributedHashIndex*) this->m_item_index;
	this->m_ro_hi_data = NULL;
	this->m_identifiers.first = NULL;
	this->m_id_index.first = NULL;
	this->m_comm = comm;
	this->m_root_pid = root_pid;
	this->m_distributed_index->m_root_pid = root_pid;
	this->m_itemset_send_buf = NULL;
	this->m_itemset_recv_buf = NULL;
	this->SYNIDG_RECV_BUF_SIZE = 409600;
	this->SYNIDB_BUF_SIZE = 409600;
	this->SYNRIG_RECV_BUF_SIZE = 409600;
	this->SYNRIB_BUF_SIZE = 409600;
	this->PHIS_BUF_SIZE = 40960;
	this->PHIR_BUF_SIZE = 40960;
	this->GENG_RECV_BUF_SIZE = 4096000;
	this->FRQGENG_RECV_BUF_SIZE = 4096;
	this->m_device_id = 0;
#ifdef OMP
	this->DEFAULT_OMP_NUM_THREADS = 4;
#endif
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::~ParallelHiApriori() {
	if (this->m_ro_hi_data != NULL) {
		delete this->m_ro_hi_data;
		this->m_ro_hi_data = NULL;
	}
	if (this->m_identifiers.first != NULL) {
		delete[] this->m_identifiers.first;
		this->m_identifiers.first = NULL;
	}
	if (this->m_id_index.first != NULL) {
		delete[] this->m_id_index.first;
		this->m_id_index.first = NULL;
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

	if (this->m_minsup <= 0 || this->m_minconf <= 0) {
		return false;
	}

	//读取数据集
	this->m_extractor->read_data(true);

	//设置缓冲区大小
	MPIDocTextExtractor* extractor = (MPIDocTextExtractor*) this->m_extractor;
	unsigned int word_num = extractor->m_record_size / extractor->WORD_SIZE;
	SYNIDG_RECV_BUF_SIZE = extractor->m_record_size + word_num * POS_SIZE;
	SYNIDB_BUF_SIZE = numprocs * SYNIDG_RECV_BUF_SIZE;
	SYNRIG_RECV_BUF_SIZE = extractor->m_record_num * (256 + 8); //文件名大小+文件ID
	SYNRIB_BUF_SIZE = numprocs * SYNRIG_RECV_BUF_SIZE;

	MPIDHashIndex* index = (MPIDHashIndex*) this->m_distributed_index;
	unsigned int avg_bucket_size = 10 * 0.5;
	index->SYNG_RECV_BUF_SIZE = 4 * (1 + word_num / avg_bucket_size);
	index->SYNB_BUF_SIZE = 4 * (1 + 2 * numprocs);
	unsigned int max_synata = numeric_limits<int>::max() / numprocs;
	unsigned long long int synata = (unsigned long long int) 4
			+ index->SYNG_RECV_BUF_SIZE + 4 * 3 * word_num
			+ 1 * (extractor->m_record_size + 32 * word_num)
			+ 4 * extractor->m_record_num * word_num;
	if (synata > max_synata) {
		index->SYNATA_BUF_SIZE = max_synata;
	} else {
		index->SYNATA_BUF_SIZE = (unsigned int) synata;
	}
	index->CONG_RECV_BUF_SIZE = index->SYNATA_BUF_SIZE;
	index->CONB_BUF_SIZE = numprocs * index->CONG_RECV_BUF_SIZE;

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

#ifdef OMP
	//决定使用哪个设备
#if MPI_VERSION >=3
	int local_pid, local_numprocs;
	MPI_Comm local_comm;
	MPI_Comm_split_type(m_comm, MPI_COMM_TYPE_SHARED, 0, NULL, &local_comm);
	MPI_Comm_rank(local_comm, &local_pid);
	MPI_Comm_size(local_comm, &local_numprocs);
	int dev_num = _Offload_number_of_devices();

	if (local_pid >= dev_num) {
		this->m_device_id = dev_num;

		int pcpu_num = 0;
		const char *cmd_get_num_pcores = "cat /proc/cpuinfo | grep \"cpu cores\" | uniq | awk -F \": \" \'{print $2}\'";
		FILE *fd = popen(cmd_get_num_pcores, "r");
		fscanf(fd, "%d", &pcpu_num);
		pclose(fd);
		int lcpu_num = 0;
		const char *cmd_get_num_lcores = "cat /proc/cpuinfo | grep \"processor\" | wc -l";
		fd = popen(cmd_get_num_lcores, "r");
		fscanf(fd, "%d", &lcpu_num);
		pclose(fd);

		int cpu_numprocs = local_numprocs - dev_num;
		if (cpu_numprocs != pcpu_num) {
//			printf("It's recommended to run with %d MPI process!\n", pcpu_num + dev_num);
			this->DEFAULT_OMP_NUM_THREADS = 3 * lcpu_num / cpu_numprocs;
		} else {
			this->DEFAULT_OMP_NUM_THREADS = 2 * lcpu_num / pcpu_num;
		}
	} else {
		this->m_device_id = local_pid % dev_num;
		this->DEFAULT_OMP_NUM_THREADS = 240;
	}
#else
	this->m_device_id = pid % (_Offload_number_of_devices() + 1);
#endif
#endif

	//生成只读索引
//	printf("start gen ro index\n");
	this->gen_ro_index();
//	printf("end gen ro index\n");

#ifdef OCL
	//初始化OpenCL环境
	this->init_opencl();
	//决定使用哪个OpenCL设备
#if MPI_VERSION >=3
	int local_pid, local_numprocs;
	MPI_Comm local_comm;
	MPI_Comm_split_type(m_comm, MPI_COMM_TYPE_SHARED, 0, NULL, &local_comm);
	MPI_Comm_rank(local_comm, &local_pid);
	MPI_Comm_size(local_comm, &local_numprocs);

	if (local_pid < this->m_cl_devices.size()) {
		this->m_device_id = local_pid % this->m_cl_devices.size();
	} else {
		float total_perfs = std::accumulate(this->m_cl_device_perfs.begin(),
				this->m_cl_device_perfs.end(), 0);
		vector<float> perfs_presum;
		float perf_presum = 0;
		for (unsigned int i = 0; i < this->m_cl_device_perfs.size(); i++) {
			perfs_presum.push_back(perf_presum);
			perf_presum = perf_presum + this->m_cl_device_perfs[i] / total_perfs;
		}
		perfs_presum.push_back(1);

		pair<unsigned int, bool> device_id = b_search<float>(perfs_presum,
				(local_pid - this->m_cl_devices.size() + 1.0f) / (local_numprocs - this->m_cl_devices.size()));
		this->m_device_id = device_id.first - 1;
	}
#else
	this->m_device_id = pid % this->m_cl_devices.size();
#endif
#endif

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
				this->m_frequent_itemsets->at(i))) {
			return false;
		}
//		printf("process %u after f%u gen\n", pid, i + 2);
//		MPI_Barrier(m_comm);

		//估算缓冲区大小
		unsigned int max_phir = numeric_limits<int>::max();
		unsigned long long int phir =
				4096
						+ 4
								* (2
										+ pow(0.8,
												this->m_frequent_itemsets->at(i).get_term_num())
												* combine(word_num,
														this->m_frequent_itemsets->at(
																i).get_term_num())
												* (this->m_frequent_itemsets->at(
														i).get_term_num() + 1));
		if (phir > max_phir) {
			PHIR_BUF_SIZE = max_phir;
		} else {
			PHIR_BUF_SIZE = (unsigned int) phir;
		}
		unsigned long long int phis = phir + MPI_BSEND_OVERHEAD;
		if (phis > max_phir) {
			PHIS_BUF_SIZE = max_phir;
		} else {
			PHIS_BUF_SIZE = (unsigned int) phis;
		}

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

#ifdef OCL
	//清理OpenCL环境
	this->release_opencl();
#endif

	return true;
}

#ifdef OMP
template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_frq_gen(
		KItemsets& frq_itemset, KItemsets& prv_frq1, KItemsets& prv_frq2) {
	if (this->m_device_id == _Offload_number_of_devices()) {
		const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets1 =
		prv_frq1.get_itemsets();
		const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets2 =
		prv_frq2.get_itemsets();
		if (prv_frq_itemsets1.size() == 0 || prv_frq_itemsets2.size() == 0) {
			return true;
		}
		bool distinct = true;
		if (&prv_frq1 == &prv_frq2) {
			distinct = false;
		}

		//声明部分要在OpenMP中用到的变量
		unsigned int prv_frq_size1 = prv_frq_itemsets1.size();
		unsigned int prv_frq_size2 = prv_frq_itemsets2.size();
		unsigned int prv_frq_term_num1 = prv_frq1.get_term_num();
		unsigned int prv_frq_term_num2 = prv_frq2.get_term_num();
		unsigned int frq_term_num = frq_itemset.get_term_num();

		//把输入数据转化成矩阵以便在并行区域随机读取
		unsigned int* frq_itemset_list1 =
		new unsigned int[prv_frq_itemsets1.size() * prv_frq1.get_term_num()];
		unsigned int* frq_itemset_list2 =
		new unsigned int[prv_frq_itemsets2.size() * prv_frq2.get_term_num()];

		unsigned int i = 0;
		for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter1 =
				prv_frq_itemsets1.begin(); prv_frq_iter1 != prv_frq_itemsets1.end();
				prv_frq_iter1++, i++) {
			memcpy(frq_itemset_list1 + i * prv_frq1.get_term_num(), prv_frq_iter1->first.data(),
					prv_frq1.get_term_num() * sizeof(unsigned int));
		}
		i = 0;
		for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter2 =
				prv_frq_itemsets2.begin(); prv_frq_iter2 != prv_frq_itemsets2.end();
				prv_frq_iter2++, i++) {
			memcpy(frq_itemset_list2 + i * prv_frq2.get_term_num(), prv_frq_iter2->first.data(),
					prv_frq2.get_term_num() * sizeof(unsigned int));
		}

		/*使用OpenMP多线程并行化*/
		//设置线程数
		unsigned int numthreads = DEFAULT_OMP_NUM_THREADS;
		//估算缓冲区大小
		FRQGENG_RECV_BUF_SIZE = max((int)(prv_frq_size1 * prv_frq_size2 / numthreads), 5) * (1 + frq_itemset.get_term_num());
		//初始化归并结果缓冲区
		unsigned int* frq_itemset_buf = new unsigned int[numthreads
		* FRQGENG_RECV_BUF_SIZE];
		unsigned int* frq_itemset_num = new unsigned int[numthreads];
		memset(frq_itemset_buf, 0,
				numthreads * FRQGENG_RECV_BUF_SIZE * sizeof(unsigned int));
		memset(frq_itemset_num, 0, numthreads * sizeof(unsigned int));

		/* 潜在频繁项集生成 */
		//声明临时变量
		unsigned int minsup_count = this->m_minsup_count;
		unsigned int buf_size = FRQGENG_RECV_BUF_SIZE;
		RODynamicHashIndexData* ro_index_data =
		(RODynamicHashIndexData*) this->m_ro_hi_data;
		unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
		*l2_index_size, *identifiers_size, *id_index, *id_index_size;
		unsigned char* l2_index;
		char* identifiers;
		ro_index_data->fill_memeber_data(&d, &data, &data_size, &l1_index,
				&l1_index_size, &l2_index, &l2_index_size);
		identifiers = this->m_identifiers.first;
		identifiers_size = &this->m_identifiers.second;
		id_index = this->m_id_index.first;
		id_index_size = &this->m_id_index.second;

#pragma omp parallel for num_threads(numthreads) shared(frq_itemset_buf, frq_itemset_num)
		for (unsigned int k = 0; k < prv_frq_size1 * prv_frq_size2; k++) {
			unsigned int tid = omp_get_thread_num();
			unsigned int i = k / prv_frq_size2;
			unsigned int j = k % prv_frq_size2;
			if (!distinct && i >= j) {
				continue;
			}

			/************************** 项目集连接与过滤 **************************/
			//求并集
			unsigned int* k_itemset = NULL;
			if (prv_frq_term_num1 == 1 || prv_frq_term_num2 == 1) { //F(k-1)×F(1)
				k_itemset = union_set_mic(frq_itemset_list1 + i * prv_frq_term_num1, prv_frq_term_num1, frq_itemset_list2 + j * prv_frq_term_num2, prv_frq_term_num2);
			} else if (prv_frq_term_num1 == prv_frq_term_num2) { //F(k-1)×F(k-1)
				k_itemset = union_eq_set_mic(frq_itemset_list1 + i * prv_frq_term_num1, prv_frq_term_num1, frq_itemset_list2 + j * prv_frq_term_num2, prv_frq_term_num2);
			}

			//构建只读索引类对象
			RODynamicHashIndex ro_index(d, data, data_size, l1_index,
					l1_index_size, l2_index, l2_index_size, simple_hash_mic);

			//过滤并保存潜在频繁项集
			unsigned int support;
			if (k_itemset != NULL && k_itemset[0] == frq_term_num
					&& phi_filter_mic(k_itemset, &support, &ro_index,
							identifiers, *identifiers_size, id_index,
							*id_index_size, minsup_count)) {
				unsigned int* offset = frq_itemset_buf
				+ tid * buf_size
				+ frq_itemset_num[tid] * (frq_term_num + 1);
				memcpy(offset, k_itemset + 1,
						k_itemset[0] * sizeof(unsigned int));
				memcpy(offset + frq_term_num, &support,
						1 * sizeof(unsigned int));
				frq_itemset_num[tid]++;
				delete[] k_itemset;
			}
		}

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

		delete[] frq_itemset_list1;
		delete[] frq_itemset_list2;
		delete[] frq_itemset_buf;
		delete[] frq_itemset_num;

		return true;
	} else {
		const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets1 =
		prv_frq1.get_itemsets();
		const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets2 =
		prv_frq2.get_itemsets();
		if (prv_frq_itemsets1.size() == 0 || prv_frq_itemsets2.size() == 0) {
			return true;
		}
		bool distinct = true;
		if (&prv_frq1 == &prv_frq2) {
			distinct = false;
		}

		//把部分要在MIC卡上用到的变量存放在临时变量
		unsigned int prv_frq_size1 = prv_frq_itemsets1.size();
		unsigned int prv_frq_size2 = prv_frq_itemsets2.size();
		unsigned int prv_frq_term_num1 = prv_frq1.get_term_num();
		unsigned int prv_frq_term_num2 = prv_frq2.get_term_num();
		unsigned int frq_term_num = frq_itemset.get_term_num();
		//把输入数据转化成矩阵以便在并行区域随机读取
		unsigned int* frq_itemset_list1 =
		new unsigned int[prv_frq_itemsets1.size() * prv_frq1.get_term_num()];
		unsigned int* frq_itemset_list2 =
		new unsigned int[prv_frq_itemsets2.size() * prv_frq2.get_term_num()];

		unsigned int i = 0;
		for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter1 =
				prv_frq_itemsets1.begin(); prv_frq_iter1 != prv_frq_itemsets1.end();
				prv_frq_iter1++, i++) {
			memcpy(frq_itemset_list1 + i * prv_frq1.get_term_num(), prv_frq_iter1->first.data(),
					prv_frq1.get_term_num() * sizeof(unsigned int));
		}
		i = 0;
		for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq_iter2 =
				prv_frq_itemsets2.begin(); prv_frq_iter2 != prv_frq_itemsets2.end();
				prv_frq_iter2++, i++) {
			memcpy(frq_itemset_list2 + i * prv_frq2.get_term_num(), prv_frq_iter2->first.data(),
					prv_frq2.get_term_num() * sizeof(unsigned int));
		}

		/*使用OpenMP多线程并行化*/
		//设置线程数
		unsigned int numthreads = DEFAULT_OMP_NUM_THREADS;
		//估算缓冲区大小
		FRQGENG_RECV_BUF_SIZE = max((int)(prv_frq_size1 * prv_frq_size2 / numthreads), 5) * (1 + frq_itemset.get_term_num());
		//初始化归并结果缓冲区
		unsigned int* frq_itemset_buf = new unsigned int[numthreads
		* FRQGENG_RECV_BUF_SIZE];
		unsigned int* frq_itemset_num = new unsigned int[numthreads];
//		memset(frq_itemset_buf, 0,
//			numthreads * FRQGENG_RECV_BUF_SIZE * sizeof(unsigned int));
//		memset(frq_itemset_num, 0, numthreads * sizeof(unsigned int));

		/* 潜在频繁项集生成 */
		//声明临时变量
		unsigned int minsup_count = this->m_minsup_count;
		unsigned int buf_size = FRQGENG_RECV_BUF_SIZE;
		RODynamicHashIndexData* ro_index_data =
		(RODynamicHashIndexData*) this->m_ro_hi_data;
		unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
		*l2_index_size, *identifiers_size, *id_index, *id_index_size;
		unsigned char* l2_index;
		char* identifiers;
		ro_index_data->fill_memeber_data(&d, &data, &data_size, &l1_index,
				&l1_index_size, &l2_index, &l2_index_size);
		identifiers = this->m_identifiers.first;
		identifiers_size = &this->m_identifiers.second;
		id_index = this->m_id_index.first;
		id_index_size = &this->m_id_index.second;
//		printf(
//			"reuse out mic: addrs d:%p, data:%p, data_size:%p, l1_index:%p, l1_index_size:%p, l2_index:%p, l2_index_size:%p, identifiers:%p, identifiers_size:%p, id_index:%p, id_index_size:%p\n",
//			d, data, data_size, l1_index, l1_index_size, l2_index,
//			l2_index_size, identifiers, identifiers_size, id_index,
//			id_index_size);
#pragma offload target(mic:this->m_device_id)\
		/* 业务数据：OUT(频繁项集缓冲区以及缓冲区大小)  IN(两个待连接的项集以及各自的项集数量和项集维度，最小支持度计数) */\
		out(frq_itemset_buf:length(numthreads * FRQGENG_RECV_BUF_SIZE))\
		out(frq_itemset_num:length(numthreads))\
		in(frq_itemset_list1:length(prv_frq_size1 * prv_frq_term_num1))\
		in(frq_itemset_list2:length(prv_frq_size2 * prv_frq_term_num2))\
		in(prv_frq_size1)\
		in(prv_frq_size2)\
		in(prv_frq_term_num1)\
		in(prv_frq_term_num2)\
		in(distinct)\
		in(frq_term_num)\
		in(minsup_count)\
		in(buf_size)\
		in(numthreads)\
		/* 索引数据 */\
		in(d:REUSE)\
		in(data:REUSE)\
		in(data_size:REUSE)\
		in(l1_index:REUSE)\
		in(l1_index_size:REUSE)\
		in(l2_index:REUSE)\
		in(l2_index_size:REUSE)\
		in(identifiers:REUSE)\
		in(identifiers_size:REUSE)\
		in(id_index:REUSE)\
		in(id_index_size:REUSE)
		{
			memset(frq_itemset_buf, 0,
					numthreads * buf_size * sizeof(unsigned int));
			memset(frq_itemset_num, 0, numthreads * sizeof(unsigned int));
#pragma omp parallel for num_threads(numthreads) shared(frq_itemset_buf, frq_itemset_num)
			for (unsigned int k = 0; k < prv_frq_size1 * prv_frq_size2; k++) {
				unsigned int tid = omp_get_thread_num();
//				printf("thread:%u is running\n", tid);
				unsigned int i = k / prv_frq_size2;
				unsigned int j = k % prv_frq_size2;
				if (!distinct && i >= j) {
					continue;
				}

				/************************** 项目集连接与过滤 **************************/
				//求并集
//				printf("start calc union\n");
				unsigned int* k_itemset = NULL;
				if (prv_frq_term_num1 == 1 || prv_frq_term_num2 == 1) { //F(k-1)×F(1)
					k_itemset = union_set_mic(frq_itemset_list1 + i * prv_frq_term_num1, prv_frq_term_num1, frq_itemset_list2 + j * prv_frq_term_num2, prv_frq_term_num2);
				} else if (prv_frq_term_num1 == prv_frq_term_num2) { //F(k-1)×F(k-1)
					k_itemset = union_eq_set_mic(frq_itemset_list1 + i * prv_frq_term_num1, prv_frq_term_num1, frq_itemset_list2 + j * prv_frq_term_num2, prv_frq_term_num2);
				}
//				printf("end calc union\n");

				//构建只读索引类对象
				RODynamicHashIndex ro_index(d, data, data_size, l1_index,
						l1_index_size, l2_index, l2_index_size, simple_hash_mic);

				//过滤并保存潜在频繁项集
				unsigned int support;
				if (k_itemset != NULL && k_itemset[0] == frq_term_num
						&& phi_filter_mic(k_itemset, &support, &ro_index,
								identifiers, *identifiers_size, id_index,
								*id_index_size, minsup_count)) {
//					printf("start write result\n");
					unsigned int* offset = frq_itemset_buf
					+ tid * buf_size
					+ frq_itemset_num[tid] * (frq_term_num + 1);
					memcpy(offset, k_itemset + 1,
							k_itemset[0] * sizeof(unsigned int));
					memcpy(offset + frq_term_num, &support,
							1 * sizeof(unsigned int));
					frq_itemset_num[tid]++;
//					printf("end write result\n");
					delete[] k_itemset;
				}
			}
		}

//		printf("start gather result\n");
		//合并结果
//		map<string, bool> itemset_map;
		for (unsigned int i = 0; i < numthreads; i++) {
			for (unsigned int j = 0; j < frq_itemset_num[i]; j++) {
				unsigned int* offset = frq_itemset_buf + i * FRQGENG_RECV_BUF_SIZE
				+ j * (frq_itemset.get_term_num() + 1);
				vector<unsigned int> itemset(offset,
						offset + frq_itemset.get_term_num());
				unsigned int support = 0;
				memcpy(&support, offset + frq_itemset.get_term_num(),
						1 * sizeof(unsigned int));

//				if (!this->hi_filter(&itemset, &support)) {
//					printf("wrong itemset:%s\n",
//							ivtoa((int*) itemset.data(),
//									itemset.size(), ","));
//				}
//				map<string, bool>::iterator iter = itemset_map.find(
//						string(
//								ivtoa((int*) itemset.data(),
//										itemset.size(), ",")));
//				if (iter != itemset_map.end()) {
//					printf("duplicate:%s, orig:%s\n",
//							ivtoa((int*) itemset.data(),
//									itemset.size(), ","),
//							iter->first.data());
//					continue;
//				} else {
//					itemset_map.insert(
//							map<string, bool>::value_type(
//									string(
//											ivtoa((int*) itemset.data(),
//													itemset.size(), ",")), 0));
//				}

				frq_itemset.push(itemset, support);
				this->logItemset("Frequent", itemset.size(), itemset, support);
			}
		}
//		printf("end gather result\n");
		delete[] frq_itemset_list1;
		delete[] frq_itemset_list2;
		delete[] frq_itemset_buf;
		delete[] frq_itemset_num;

		return true;
	}
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_filter(
		vector<unsigned int>* k_itemset, unsigned int* support,
		ROHashIndex* ro_index, char* identifiers, unsigned int identifiers_size,
		unsigned int* id_index, unsigned int id_index_size,
		unsigned int minsup_count) {
	char **keys = new char*[k_itemset->size()];
	if (0 == this->m_minsup_count)
	this->m_minsup_count = 1;
	unsigned int *result;
	for (unsigned int j = 0; j < k_itemset->size(); j++) {
		keys[j] = identifiers + id_index[k_itemset->at(j)];
//		printf("id:%s\t", keys[j]);
	}
//	printf("start calc intersect\n");
	result = ro_index->get_intersect_records((const char **) keys,
			k_itemset->size());
//	printf("end calc intersect\n");
	*support = result[0];
	delete[] keys;
	delete[] result;

	return *support >= this->m_minsup_count;
}

#elif defined(OCL)

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_frq_gen(
		KItemsets& frq_itemset, KItemsets& prv_frq1, KItemsets& prv_frq2) {
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets1 =
			prv_frq1.get_itemsets();
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets2 =
			prv_frq2.get_itemsets();
	vector<vector<unsigned int>*> candidate_itemset;
	vector<unsigned int>* k_itemset = NULL;

	//每个项集需要进行OpenCL计算的次数
	unsigned int pass = ceil(
			log(max(prv_frq1.get_term_num(), prv_frq2.get_term_num()) + 1)
					/ log(2));
	//用长度为pass+1的数组来存储每次计算的输入输出（每次的输出是下一次的输入）
	vector<vector<pair<unsigned long long, vector<unsigned long long>*> >*> in_output[pass
			+ 1];

	//OpenCL输入计算数据
	vector<unsigned long long>* data = new vector<unsigned long long>;
	//标记key所对应的record数据是否已加入OpenCL数据，避免重复
	std::map<unsigned int, unsigned long long> data_map;

	/* 潜在频繁项集生成 */
	unsigned int* frq2_offset = new unsigned int[prv_frq_itemsets1.size()];
	if (&prv_frq1 != &prv_frq2) {
		memset(frq2_offset, 0, prv_frq_itemsets1.size() * sizeof(unsigned int));
	} else {
		for (unsigned int i = 0; i < prv_frq_itemsets1.size(); i++) {
			frq2_offset[i] = i + 1;
		}
	}
	unsigned int t = 0;
	for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq1_iter =
			prv_frq_itemsets1.begin(); prv_frq1_iter != prv_frq_itemsets1.end();
			prv_frq1_iter++, t++) {
		map<vector<unsigned int>, unsigned int>::const_iterator prv_frq2_iter =
				prv_frq_itemsets2.begin();
		for (unsigned int s = 0; s < frq2_offset[t]; s++) {
			prv_frq2_iter++;
		}
		for (; prv_frq2_iter != prv_frq_itemsets2.end(); prv_frq2_iter++) {

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

			//过滤潜在频繁项集并准备第一次调用OpenCL的数据
			if (k_itemset != NULL
					&& k_itemset->size() == frq_itemset.get_term_num()) {
				char **keys = new char*[k_itemset->size()];
				for (unsigned int i = 0; i < k_itemset->size(); i++) {
					keys[i] =
							this->m_item_details[k_itemset->at(i)].m_identifier;
				}

				vector<pair<unsigned long long, vector<unsigned long long>*> >* ioput =
						new vector<
								pair<unsigned long long,
										vector<unsigned long long>*> >;

				//每个key对应于OpenCL数据区域的偏移量
				unsigned long long data_offset[k_itemset->size()];
				for (unsigned int i = 0; i < k_itemset->size(); i++) {
					std::map<unsigned int, unsigned long long>::iterator data_iter =
							data_map.find(k_itemset->at(i));
					if (data_iter != data_map.end()) {
						data_offset[i] = data_iter->second;
					} else {
						data_offset[i] = data->size();
						data_map[k_itemset->at(i)] = data->size();
						unsigned int record_num =
								this->m_item_index->get_mark_record_num(keys[i],
										strlen(keys[i]));
						unsigned int record[record_num + 1];
						record[0] = record_num;
						this->m_item_index->find_record(record + 1, keys[i],
								strlen(keys[i]));
						data->insert(data->end(), record,
								record + record_num + 1);
					}
					//初始偏移量
					unsigned long long data_length = data->at(data_offset[i]);
					unsigned long long offset_array[data_length];
					for (unsigned int j = 0; j < data_length; j++) {
						offset_array[j] = j + 1;
					}
					ioput->push_back(
							pair<unsigned long long, vector<unsigned long long>*>(
									data_offset[i],
									new vector<unsigned long long>(offset_array,
											offset_array + data_length)));
				}
				in_output[0].push_back(ioput);

				//暂存候选频繁项集
				candidate_itemset.push_back(k_itemset);

//				if (strcmp(
//						(const char*) ivtoa((int*) k_itemset->data(),
//								k_itemset->size()), "87105122") == 0) {
//					printf("87105122 from: %s and %s\n",
//							ivtoa((int*) prv_frq1_iter->first.data(),
//									prv_frq1_iter->first.size()),
//							ivtoa((int*) prv_frq2_iter->first.data(),
//									prv_frq2_iter->first.size()));
//				}

				delete[] keys;
			} else {
				delete k_itemset;
			}
		}
	}

	//没有产生计算任务
	if (data->size() == 0) {
		return true;
	}

	//先把data数据传输到OpenCL设备
//	cl_int err = CL_SUCCESS;
//	cl_mem cl_data = clCreateBuffer(this->m_cl_contexts[0],
//			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
//			sizeof(unsigned long long) * data->size(), data->data(), &err);
//	OPENCL_CHECK_ERRORS(err);

	bool succeed = true;
	//多个数组求交集需要采用2路归并的方法
	for (unsigned int p = 0; p < pass; p++) {
		//OpenCL输入辅助数据
		vector<unsigned long long>* in_indice = new vector<unsigned long long>;
		vector<unsigned long long>* in_offset = new vector<unsigned long long>;
		vector<unsigned long long>* out_indice = new vector<unsigned long long>;
		vector<unsigned long long>* out_offset = new vector<unsigned long long>;
		vector<unsigned long long>* out_offset_indice = new vector<
				unsigned long long>;
		vector<unsigned long long>* task_indice = new vector<unsigned long long>;
		//统计总共需要完成的细粒度任务数
		size_t total_work_load = 0;
		//统计总共需要完成的细粒度任务数
		size_t total_work_task = 0;

		for (unsigned int i = 0; i < in_output[p].size(); i++) {
			//项集中两个项为一组求交集
			unsigned int partition = in_output[p][i]->size() / 2;
			for (unsigned int k = 0; k < partition; k++) {
				//每组的第一个key(里面的元素分别在第二个key的记录中进行查询)对应的记录数量
				unsigned int in_length =
						in_output[p][i]->at(2 * k).second->size();
				//累加任务数量
				size_t work_load = in_length;
				total_work_load += work_load;

				//辨识每个细粒度任务对应的粗粒度任务
				task_indice->insert(task_indice->end(), work_load,
						total_work_task);

				//构建in_indice
				in_indice->insert(in_indice->end(), work_load,
						in_output[p][i]->at(2 * k).first);
				//构建in_offset
				in_offset->insert(in_offset->end(),
						in_output[p][i]->at(2 * k).second->begin(),
						in_output[p][i]->at(2 * k).second->end());
				//构建out_indice
				out_indice->insert(out_indice->end(), work_load,
						in_output[p][i]->at(2 * k + 1).first);

				//构建out_offset和out_offset_indice
				unsigned long long out_offset_index = out_offset->size();
				out_offset->push_back(
						in_output[p][i]->at(2 * k + 1).second->size());
				out_offset->insert(out_offset->end(),
						in_output[p][i]->at(2 * k + 1).second->begin(),
						in_output[p][i]->at(2 * k + 1).second->end());
				out_offset_indice->insert(out_offset_indice->end(), work_load,
						out_offset_index);
				//累加粗粒度任务数量
				total_work_task += 1;
			}
			//每个项集在本层的输出作为下一层归并的输入
			vector<pair<unsigned long long, vector<unsigned long long>*> >* next_io =
					new vector<
							pair<unsigned long long, vector<unsigned long long>*> >();
			//非偶数项项集的最后一项直接加入下一轮的输入
			if (2 * partition < in_output[p][i]->size()) {
				next_io->push_back(
						pair<unsigned long long, vector<unsigned long long>*>(
								in_output[p][i]->back().first,
								new vector<unsigned long long>(
										*in_output[p][i]->back().second)));
			}
			in_output[p + 1].push_back(next_io);
		}
		assert(total_work_load == in_indice->size());
		assert(total_work_load == in_offset->size());
		assert(total_work_load == out_indice->size());
		assert(total_work_load == out_offset_indice->size());

		//使用OpenCL进行计算
		vector<vector<unsigned long long> > result;
		succeed &= phi_filter(&result, data, in_indice, in_offset, out_indice,
				out_offset, out_offset_indice, task_indice, total_work_load,
				total_work_task);

		//验证结果
//		vector<vector<unsigned long long> > result_cpu;
//		vector<unsigned long long> empty_v;
//		for (unsigned int i = 0; i < total_work_task; i++) {
//			result_cpu.push_back(empty_v);
//		}
//		for (unsigned int i = 0; i < total_work_load; i++) {
//			unsigned long long o_indice = out_indice->at(i);
//			unsigned long long target = *(data->begin() + in_indice->at(i)
//					+ in_offset->at(i));
//			unsigned long long offset_indice = out_offset_indice->at(i);
//			vector<unsigned long long> space_offset =
//					vector<unsigned long long>(
//							out_offset->begin() + offset_indice + 1,
//							out_offset->begin() + offset_indice + 1
//									+ out_offset->at(offset_indice));
//			vector<unsigned long long> space = vector<unsigned long long>(
//					data->begin() + o_indice,
//					data->begin() + o_indice + 1 + data->at(o_indice));
//			for (unsigned long long t = 0; t < space_offset.size(); t++) {
//				if (space[space_offset[t]] == target) {
//					result_cpu[task_indice->at(i)].push_back(space_offset[t]);
//				}
//			}
//		}
//		assert(result.size() == result_cpu.size());
//		for (unsigned int i = 0; i < result.size(); i++) {
//			assert(result[i].size() == result_cpu[i].size());
//			for (unsigned int j = 0; j < result[i].size(); j++) {
//				assert(result[i][j] == result_cpu[i][j]);
//			}
//		}

		//处理结果
		unsigned int partition = in_output[p][0]->size() / 2;
		for (unsigned int i = 0; i < result.size(); i++) {
			in_output[p + 1][i / partition]->push_back(
					pair<unsigned long long, vector<unsigned long long>*>(
							in_output[p][i / partition]->at(
									2 * (i % partition) + 1).first,
							new vector<unsigned long long>(result[i].begin(),
									result[i].end())));
		}

		for (unsigned int j = 0; j < in_output[p].size(); j++) {
			if (in_output[p][j] != NULL) {
				for (unsigned int k = 0; k < in_output[p][j]->size(); k++) {
					if (in_output[p][j]->at(k).second != NULL) {
						delete in_output[p][j]->at(k).second;
						in_output[p][j]->at(k).second = NULL;
					}
				}
				delete in_output[p][j];
				in_output[p][j] = NULL;
			}
		}
		delete in_indice;
		delete in_offset;
		delete out_indice;
		delete out_offset;
		delete out_offset_indice;
		delete task_indice;
	}
	assert(in_output[pass].size() == candidate_itemset.size());

//	map<string, bool> itemset_map;
	//保存频繁项集
	for (unsigned int i = 0; i < candidate_itemset.size(); i++) {
//		map<string, bool>::iterator iter = itemset_map.find(
//				string(
//						ivtoa((int*) candidate_itemset[i]->data(),
//								candidate_itemset[i]->size(), ",")));
//		if (iter != itemset_map.end()) {
//			printf("duplicate:%s, orig:%s\n",
//					ivtoa((int*) candidate_itemset[i]->data(),
//							candidate_itemset[i]->size(), ","),
//					iter->first.data());
//			continue;
//		} else {
//			itemset_map.insert(
//					map<string, bool>::value_type(
//							string(
//									ivtoa((int*) candidate_itemset[i]->data(),
//											candidate_itemset[i]->size(), ",")), 0));
//		}
		unsigned int support = in_output[pass][i]->at(0).second->size();
		if (support >= this->m_minsup_count) {
			//验证结果
//			unsigned int support_cpu = 0;
//			if (!this->hi_filter(candidate_itemset[i], &support_cpu)) {
//				printf("wrong itemset:%s\n",
//						ivtoa((int*) candidate_itemset[i]->data(),
//								candidate_itemset[i]->size(), ","));
//				char **keys = new char*[candidate_itemset[i]->size()];
//				for (unsigned int i = 0; i < candidate_itemset[i]->size();
//						i++) {
//					keys[i] =
//							this->m_item_details[candidate_itemset[i]->at(i)].m_identifier;
//					unsigned int records[this->m_item_index->get_mark_record_num(
//							keys[i], strlen(keys[i]))];
//					unsigned int num = this->m_item_index->find_record(records,
//							keys[i], strlen(keys[i]));
//					for (unsigned int j = 0; j < num; j++) {
//						printf("%u, ", records[j]);
//					}
//
//					printf("\n");
//				}
//				printf("result:\n");
//				for (unsigned int j = 0; j < pass + 1; j++) {
//					printf("\tlevel %d:\n", j);
//					for (unsigned int k = 0; k < in_output[j][i]->size(); k++) {
//						printf("\t");
//						for (unsigned int t = 0;
//								t < in_output[j][i]->at(k).second->size();
//								t++) {
//							printf("%d,", in_output[j][i]->at(k).second->at(t));
//						}
//						printf("\n");
//					}
//				}
//			}
			frq_itemset.push(*candidate_itemset[i], support);
			this->logItemset("Frequent", candidate_itemset[i]->size(),
					*candidate_itemset[i], support);
		}
	}

	delete data;
	for (unsigned int j = 0; j < in_output[pass].size(); j++) {
		if (in_output[pass][j] != NULL) {
			for (unsigned int k = 0; k < in_output[pass][j]->size(); k++) {
				if (in_output[pass][j]->at(k).second != NULL) {
					delete in_output[pass][j]->at(k).second;
					in_output[pass][j]->at(k).second = NULL;
				}
			}
			delete in_output[pass][j];
			in_output[pass][j] = NULL;
		}
	}
	for (unsigned int i = 0; i < candidate_itemset.size(); i++) {
		delete candidate_itemset[i];
	}

	return succeed;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_filter(
		vector<vector<unsigned long long> >* intersect_result,
		vector<unsigned long long>* data, vector<unsigned long long>* in_indice,
		vector<unsigned long long>* in_offset,
		vector<unsigned long long>* out_indice,
		vector<unsigned long long>* out_offset,
		vector<unsigned long long>* out_offset_indice,
		vector<unsigned long long>* task_indice, size_t total_work_load,
		size_t total_work_task) {
	unsigned int ctx_id = this->m_device_id;
	unsigned int dev_id = ctx_id;

	cl_int err = CL_SUCCESS;
	//创建OpenCL命令队列
	cl_command_queue queue = clCreateCommandQueue(this->m_cl_contexts[ctx_id],
			this->m_cl_devices[dev_id], 0, &err);
	OPENCL_CHECK_ERRORS(err);
	//向OpenCL设备传输数据
	cl_mem cl_data = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
			sizeof(unsigned long long) * data->size(), data->data(), &err);
	OPENCL_CHECK_ERRORS(err);
	cl_mem cl_in_indice = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
			sizeof(unsigned long long) * in_indice->size(), in_indice->data(),
			&err);
	OPENCL_CHECK_ERRORS(err);
	cl_mem cl_in_offset = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
			sizeof(unsigned long long) * in_offset->size(), in_offset->data(),
			&err);
	OPENCL_CHECK_ERRORS(err);
	cl_mem cl_out_indice = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
			sizeof(unsigned long long) * out_indice->size(), out_indice->data(),
			&err);
	OPENCL_CHECK_ERRORS(err);
	cl_mem cl_out_offset = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
			sizeof(unsigned long long) * out_offset->size(), out_offset->data(),
			&err);
	OPENCL_CHECK_ERRORS(err);
	cl_mem cl_out_offset_indice = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
			sizeof(unsigned long long) * out_offset_indice->size(),
			out_offset_indice->data(), &err);
	OPENCL_CHECK_ERRORS(err);
	cl_mem cl_task_indice = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
			sizeof(unsigned long long) * task_indice->size(),
			task_indice->data(), &err);
	OPENCL_CHECK_ERRORS(err);
	cl_ulong cl_work_load = total_work_load;

	//结果缓冲区，存储每个元素在目标空间里的位置，找不到标记为-1
	long long* result = new long long[total_work_load];
	cl_mem cl_result = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_WRITE_ONLY, sizeof(unsigned long long) * total_work_load,
			NULL, &err);
	OPENCL_CHECK_ERRORS(err);
	//计数结果缓冲区，存储每次求交集得到的元素个数
	unsigned int* result_count = new unsigned int[total_work_task];
	cl_mem cl_result_count = clCreateBuffer(this->m_cl_contexts[ctx_id],
			CL_MEM_WRITE_ONLY, sizeof(unsigned int) * total_work_task, NULL,
			&err);
	OPENCL_CHECK_ERRORS(err);

	//创建程序
	const char* source_path =
			"include/libyskdmu/association/phi_apriori_kernel.cl";
	int source_file = open(source_path, O_RDONLY);
	size_t src_size = lseek(source_file, 0, SEEK_END);	//计算文件大小
	src_size += 1;
	lseek(source_file, 0, SEEK_SET);	//把文件指针重新移到开始位置
	char* source = new char[src_size];	//最后一个字节是EOF
	read(source_file, source, src_size);
	memset(source + src_size - 1, 0, 1);	//字符结束符
	close(source_file);
	cl_program program = clCreateProgramWithSource(this->m_cl_contexts[ctx_id],
			1, (const char**) &source, &src_size, &err);
	err = clBuildProgram(program, 1, &this->m_cl_devices[dev_id], NULL, NULL,
			NULL);
	OPENCL_CHECK_ERRORS(err);

	//创建Kernel
	cl_kernel intersect_kernel = clCreateKernel(program, "decarl_intersect",
			&err);
	OPENCL_CHECK_ERRORS(err);

	//设置Kernel参数
	err = clSetKernelArg(intersect_kernel, 0, sizeof(cl_mem), &cl_result);
	err |= clSetKernelArg(intersect_kernel, 1, sizeof(cl_mem),
			&cl_result_count);
	err |= clSetKernelArg(intersect_kernel, 2, sizeof(cl_mem), &cl_data);
	err |= clSetKernelArg(intersect_kernel, 3, sizeof(cl_mem), &cl_in_indice);
	err |= clSetKernelArg(intersect_kernel, 4, sizeof(cl_mem), &cl_in_offset);
	err |= clSetKernelArg(intersect_kernel, 5, sizeof(cl_mem), &cl_out_indice);
	err |= clSetKernelArg(intersect_kernel, 6, sizeof(cl_mem), &cl_out_offset);
	err |= clSetKernelArg(intersect_kernel, 7, sizeof(cl_mem),
			&cl_out_offset_indice);
	err |= clSetKernelArg(intersect_kernel, 8, sizeof(cl_mem), &cl_task_indice);
	err |= clSetKernelArg(intersect_kernel, 9, sizeof(cl_ulong), &cl_work_load);
	OPENCL_CHECK_ERRORS(err);

	//执行Kernel
	const size_t local_ws = 512;	//每个work-group的work-items数量
	const size_t global_ws = ceil(1.0f * total_work_load / local_ws) * local_ws;

	err = clEnqueueNDRangeKernel(queue, intersect_kernel, 1, NULL, &global_ws,
			&local_ws, 0, NULL, NULL);
	OPENCL_CHECK_ERRORS(err);

	//读取结果
	clEnqueueReadBuffer(queue, cl_result, CL_TRUE, 0,
			sizeof(unsigned long long) * total_work_load, result, 0, NULL,
			NULL);
	clEnqueueReadBuffer(queue, cl_result_count, CL_TRUE, 0,
			sizeof(unsigned int) * total_work_task, result_count, 0, NULL,
			NULL);

	//取出交集数据在out_joiner的索引值
	intersect_result->clear();
	vector<unsigned long long> empty_v;
	intersect_result->insert(intersect_result->end(), total_work_task, empty_v);
	for (unsigned int i = 0; i < total_work_load; i++) {
		if (result[i] != -1) {
			intersect_result->at(task_indice->at(i)).push_back(result[i]);
		}
	}

	delete[] source;
	delete[] result;
	delete[] result_count;
	clReleaseProgram(program);
	clReleaseKernel(intersect_kernel);
	clReleaseCommandQueue(queue);
	clReleaseMemObject(cl_data);
	clReleaseMemObject(cl_in_indice);
	clReleaseMemObject(cl_in_offset);
	clReleaseMemObject(cl_out_indice);
	clReleaseMemObject(cl_out_offset);
	clReleaseMemObject(cl_out_offset_indice);
	clReleaseMemObject(cl_task_indice);
	clReleaseMemObject(cl_result);
	clReleaseMemObject(cl_result_count);

	return err == CL_SUCCESS;
}

#else

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_frq_gen(
		KItemsets& frq_itemset, KItemsets& prv_frq1, KItemsets& prv_frq2) {
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets1 =
	prv_frq1.get_itemsets();
	const map<vector<unsigned int>, unsigned int>& prv_frq_itemsets2 =
	prv_frq2.get_itemsets();
	vector<unsigned int>* k_itemset = NULL;

	/* 潜在频繁项集生成 */
	unsigned int* frq2_offset = new unsigned int[prv_frq_itemsets1.size()];
	if (&prv_frq1 != &prv_frq2) {
		memset(frq2_offset, 0, prv_frq_itemsets1.size() * sizeof(unsigned int));
	} else {
		for (unsigned int i = 0; i < prv_frq_itemsets1.size(); i++) {
			frq2_offset[i] = i + 1;
		}
	}
	unsigned int t = 0;
	for (map<vector<unsigned int>, unsigned int>::const_iterator prv_frq1_iter =
			prv_frq_itemsets1.begin(); prv_frq1_iter != prv_frq_itemsets1.end();
			prv_frq1_iter++, t++) {
		map<vector<unsigned int>, unsigned int>::const_iterator prv_frq2_iter =
		prv_frq_itemsets2.begin();
		for (unsigned int s = 0; s < frq2_offset[t]; s++) {
			prv_frq2_iter++;
		}
		for (; prv_frq2_iter != prv_frq_itemsets2.end(); prv_frq2_iter++) {
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
					&& this->hi_filter(k_itemset, &support)) {
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
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::phi_filter(
		vector<unsigned int>* k_itemset, unsigned int* support) {
	char **keys = new char*[k_itemset->size()];
	if (0 == this->m_minsup_count)
	this->m_minsup_count = 1;
	unsigned int *result;
	for (unsigned int j = 0; j < k_itemset->size(); j++) {
		keys[j] = this->m_item_details[k_itemset->at(j)].m_identifier;
	}

	RODynamicHashIndexData* ro_index_data =
	(RODynamicHashIndexData*) this->m_ro_hi_data;
	unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
	*l2_index_size, *identifiers_size, *id_index, *id_index_size;
	unsigned char* l2_index;
	ro_index_data->fill_memeber_data(&d, &data, &data_size, &l1_index,
			&l1_index_size, &l2_index, &l2_index_size);
	RODynamicHashIndex ro_index(d, data, data_size, l1_index, l1_index_size,
			l2_index, l2_index_size, (HashFunc) &simple_hash);
	result = ro_index.get_intersect_records((const char **) keys,
			k_itemset->size());

	*support = result[0];
	delete[] keys;
	delete[] result;

	return *support >= this->m_minsup_count;
}
#endif

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

	//估算缓冲区大小
	MPIDocTextExtractor* extractor = (MPIDocTextExtractor*) this->m_extractor;
	unsigned int word_num = extractor->m_record_size / extractor->WORD_SIZE;
	unsigned int max_geng = numeric_limits<int>::max() / numprocs;
	//用正太分布来逼近二项展开式(1+1)^n=c(n,0)+c(n,1)+...+c(n,n)
	unsigned int miu = combine(word_num, word_num / 2);
	unsigned int lou = 3 * pow(2, word_num - 4);
	unsigned long long int sum = numeric_limits<unsigned long long int>::max();
	if (word_num < 64) {
		sum = pow(2, word_num);
	}
	unsigned long long int part_sum = sum;
	unsigned int center_deviation = abs(
			(int) word_num / 2 - (int) this->m_max_itemset_size);
	if (center_deviation > 3 * lou) {
		part_sum = 0.01 * sum;
	} else if (center_deviation > 2 * lou) {
		part_sum = 0.05 * sum;
	} else if (center_deviation > lou) {
		part_sum = 0.27 * sum;
	} else {
		part_sum = 0.68 * sum;
	}
	unsigned long long int geng = (unsigned long long int) part_sum
			* (4 * (2 + this->m_max_itemset_size) + 8);
	if (geng > max_geng) {
		GENG_RECV_BUF_SIZE = max_geng;
	} else {
		GENG_RECV_BUF_SIZE = (unsigned int) geng;
	}

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
	if (this->m_ro_hi_data != NULL) {
		delete this->m_ro_hi_data;
	}
	RODynamicHashIndexData* ro_hi_data = new RODynamicHashIndexData();
	this->m_ro_hi_data = ro_hi_data;
	bool succeed = true;
	succeed &= ro_hi_data->build(this->m_item_index);
#ifdef OMP
	succeed &= this->gen_identifiers(&this->m_identifiers.first,
			&this->m_identifiers.second, &this->m_id_index.first,
			&this->m_id_index.second);
	if (this->m_device_id == _Offload_number_of_devices()) {
		return succeed;
	}
	//把只读索引拷贝到加速卡
	unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
	*l2_index_size, *identifiers_size, *id_index, *id_index_size;
	unsigned char* l2_index;
	char* identifiers;
	ro_hi_data->fill_memeber_data(&d, &data, &data_size, &l1_index,
			&l1_index_size, &l2_index, &l2_index_size);
	identifiers = this->m_identifiers.first;
	identifiers_size = &this->m_identifiers.second;
	id_index = this->m_id_index.first;
	id_index_size = &this->m_id_index.second;
//	printf(
//			"alloc out mic: addrs d:%p, data:%p, data_size:%p, l1_index:%p, l1_index_size:%p, l2_index:%p, l2_index_size:%p, identifiers:%p, identifiers_size:%p, id_index:%p, id_index_size:%p\n",
//			d, data, data_size, l1_index, l1_index_size, l2_index,
//			l2_index_size, identifiers, identifiers_size, id_index,
//			id_index_size);
#pragma offload target(mic:this->m_device_id)\
		in(d:length(1) ALLOC)\
		in(data:length(*data_size) ALLOC)\
		in(data_size:length(1) ALLOC)\
		in(l1_index:length(*l1_index_size) ALLOC)\
		in(l1_index_size:length(1) ALLOC)\
		in(l2_index:length(*l2_index_size) ALLOC)\
		in(l2_index_size:length(1) ALLOC)\
		in(identifiers:length(*identifiers_size) ALLOC)\
		in(identifiers_size:length(1) ALLOC)\
		in(id_index:length(*id_index_size) ALLOC)\
		in(id_index_size:length(1) ALLOC)
	{
//		printf(
//				"alloc in mic: addrs dd:%p, data:%p, data_size:%p, l1_index:%p, l1_index_size:%p, l2_index:%p, l2_index_size:%p, identifiers:%p, identifiers_size:%p, id_index:%p, id_index_size:%p\n",
//				d, data, data_size, l1_index, l1_index_size, l2_index,
//				l2_index_size, identifiers, identifiers_size, id_index,
//				id_index_size);
	}
#endif
	return succeed;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::destroy_ro_index() {
	if (this->m_ro_hi_data == NULL) {
		return true;
	}
	bool succeed = true;
#ifdef OMP
	if (this->m_device_id == _Offload_number_of_devices()) {
		if (this->m_identifiers.first != NULL) {
			delete[] this->m_identifiers.first;
			this->m_identifiers.first = NULL;
		}
		if (this->m_id_index.first != NULL) {
			delete[] this->m_id_index.first;
			this->m_id_index.first = NULL;
		}
		if (this->m_ro_hi_data != NULL) {
			delete this->m_ro_hi_data;
			this->m_ro_hi_data = NULL;
		}
		return succeed;
	}
	//把加速卡的只读索引内存空间回收
	unsigned int *d, *data, *data_size, *l1_index, *l1_index_size,
	*l2_index_size, *identifiers_size, *id_index, *id_index_size;
	unsigned char* l2_index;
	char* identifiers;
	RODynamicHashIndexData* ro_hi_data =
	(RODynamicHashIndexData*) this->m_ro_hi_data;
	ro_hi_data->fill_memeber_data(&d, &data, &data_size, &l1_index,
			&l1_index_size, &l2_index, &l2_index_size);
	identifiers = this->m_identifiers.first;
	identifiers_size = &this->m_identifiers.second;
	id_index = this->m_id_index.first;
	id_index_size = &this->m_id_index.second;
//	printf(
//			"free out mic: addrs d:%p, data:%p, data_size:%p, l1_index:%p, l1_index_size:%p, l2_index:%p, l2_index_size:%p, identifiers:%p, identifiers_size:%p, id_index:%p, id_index_size:%p\n",
//			d, data, data_size, l1_index, l1_index_size, l2_index,
//			l2_index_size, identifiers, identifiers_size, id_index,
//			id_index_size);
#pragma offload target(mic:this->m_device_id)\
		in(d:FREE)\
		in(data:FREE)\
		in(data_size:FREE)\
		in(l1_index:FREE)\
		in(l1_index_size:FREE)\
		in(l2_index:FREE)\
		in(l2_index_size:FREE)\
		in(identifiers:FREE)\
		in(identifiers_size:FREE)\
		in(id_index:FREE)\
		in(id_index_size:FREE)
	{
//		printf(
//				"free in mic: addrs d:%p, data:%p, data_size:%p, l1_index:%p, l1_index_size:%p, l2_index:%p, l2_index_size:%p, identifiers:%p, identifiers_size:%p, id_index:%p, id_index_size:%p\n",
//				d, data, data_size, l1_index, l1_index_size, l2_index,
//				l2_index_size, identifiers, identifiers_size, id_index,
//				id_index_size);
	}

	if (this->m_identifiers.first != NULL) {
		delete[] this->m_identifiers.first;
		this->m_identifiers.first = NULL;
	}
	if (this->m_id_index.first != NULL) {
		delete[] this->m_id_index.first;
		this->m_id_index.first = NULL;
	}
#endif
	if (this->m_ro_hi_data != NULL) {
		delete this->m_ro_hi_data;
		this->m_ro_hi_data = NULL;
	}
	return succeed;
}

#ifdef OCL
template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::init_opencl() {
	cl_int err = CL_SUCCESS;
	//查询系统内所有可用的OpenCl平台
	cl_uint num_of_platforms = 0;
	//获取可用的平台数量
	err = clGetPlatformIDs(0, 0, &num_of_platforms);
	OPENCL_CHECK_ERRORS(err);

	cl_platform_id* platforms = new cl_platform_id[num_of_platforms];
	//获取所有平台的ID
	err = clGetPlatformIDs(num_of_platforms, platforms, 0);
	OPENCL_CHECK_ERRORS(err);

	//遍历所有平台的所有类型设备
	map<string, bool> skip_platforms;
	vector<cl_device_id>& device_ids = this->m_cl_devices;
	vector<cl_device_type>& device_types = this->m_cl_device_types;
	for (unsigned int i = 0; i < num_of_platforms; i++) {
		//获取平台名称的长度
		size_t platform_name_length = 0;
		err = clGetPlatformInfo(platforms[i], CL_PLATFORM_NAME, 0, 0,
				&platform_name_length);
		OPENCL_CHECK_ERRORS(err);
		//获取平台的名称
		char* platform_name = new char[platform_name_length];
		err = clGetPlatformInfo(platforms[i], CL_PLATFORM_NAME,
				platform_name_length, platform_name, 0);
		OPENCL_CHECK_ERRORS(err);
		//跳过忽略的平台
		if (skip_platforms.find(string(platform_name))
				!= skip_platforms.end()) {
			delete[] platform_name;
			continue;
		}

		//利用所有类型的设备
		struct {
			cl_device_type type;
			const char* name;
			cl_uint count;
		} devices[] =
				{ { CL_DEVICE_TYPE_CPU, "CL_DEVICE_TYPE_CPU", 0 },
//				{ CL_DEVICE_TYPE_GPU, "CL_DEVICE_TYPE_GPU", 0 },
						{ CL_DEVICE_TYPE_ACCELERATOR,
								"CL_DEVICE_TYPE_ACCELERATOR", 0 } };
		const int NUM_OF_DEVICE_TYPES = sizeof(devices) / sizeof(devices[0]);
		for (unsigned int j = 0; j < NUM_OF_DEVICE_TYPES; j++) {
			err = clGetDeviceIDs(platforms[i], devices[j].type, 0, 0,
					&devices[j].count);
			if (CL_DEVICE_NOT_FOUND == err) {
				// 没有找到该类型的设备
				devices[j].count = 0;
				continue;
			}
			OPENCL_CHECK_ERRORS(err);
			device_types.insert(device_types.end(), devices[j].count,
					devices[j].type);

			// 获取该类型的设备
			cl_device_id* devices_of_type = new cl_device_id[devices[j].count];
			err = clGetDeviceIDs(platforms[i], devices[j].type,
					devices[j].count, devices_of_type, 0);
			OPENCL_CHECK_ERRORS(err);
			device_ids.insert(device_ids.end(), devices_of_type,
					devices_of_type + devices[j].count);

			// 获取各个设备的信息
			for (unsigned int k = 0; k < devices[j].count; k++) {
				cl_uint compute_units;
				cl_uint clock_freq;
				cl_ulong gmem_size;
				OPENCL_GET_NUMERIC_PROPERTY(devices_of_type[k],
						CL_DEVICE_MAX_COMPUTE_UNITS, compute_units);
				OPENCL_GET_NUMERIC_PROPERTY(devices_of_type[k],
						CL_DEVICE_MAX_CLOCK_FREQUENCY, clock_freq);
				OPENCL_GET_NUMERIC_PROPERTY(devices_of_type[k],
						CL_DEVICE_GLOBAL_MEM_SIZE, gmem_size);
				this->m_cl_device_perfs.push_back(
						compute_units * clock_freq * gmem_size);
			}
			delete[] devices_of_type;
		}
		delete[] platform_name;
	}

	//创建OpenCL上下文
	for (unsigned int i = 0; i < device_ids.size(); i++) {
		this->m_cl_contexts.push_back(
				clCreateContext(0, 1, &device_ids[i], NULL, NULL, &err));
		OPENCL_CHECK_ERRORS(err);
	}

	delete[] platforms;
	return err == CL_SUCCESS;
}

template<typename ItemType, typename ItemDetail, typename RecordInfoType>
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::release_opencl() {
	for (unsigned int i = 0; i < this->m_cl_contexts.size(); i++) {
		clReleaseContext(this->m_cl_contexts[i]);
		clReleaseDevice(this->m_cl_devices[i]);
	}
	return true;
}
#endif

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
bool ParallelHiApriori<ItemType, ItemDetail, RecordInfoType>::gen_identifiers(
		char** identifiers, unsigned int* identifiers_size,
		unsigned int** id_index, unsigned int* id_index_size) {
	unsigned int total_id_len = 0;
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		total_id_len += strlen(this->m_item_details[i].m_identifier) + 1;
	}
	*identifiers_size = total_id_len;
	*identifiers = new char[total_id_len];
	*id_index_size = this->m_item_details.size();
	*id_index = new unsigned int[*id_index_size];
	unsigned int index = 0;
	for (unsigned int i = 0; i < this->m_item_details.size(); i++) {
		*(*id_index + i) = index;
		strcpy(*identifiers + index, this->m_item_details[i].m_identifier);
		index += strlen(this->m_item_details[i].m_identifier) + 1;
	}

	return true;
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
