/*
 * mpi_d_hash_index.h
 *
 *  Created on: 2013-7-10
 *      Author: Yan Shankai
 */

#ifndef MPI_D_HASH_INDEX_H_
#define MPI_D_HASH_INDEX_H_

#include <mpi.h>
#include "libyskdmu/index/distributed_hash_index.h"

struct SynGatherMsg {
	unsigned int global_deep;
	unsigned int* statistics;
};

struct SynBcastMsg {
	unsigned int global_deep;
	unsigned int* catalog_offset;
	unsigned int* catalog_size;
};

class MPIDHashIndex: public DynamicHashIndex, public DistributedHashIndex {
public:
	MPIDHashIndex(MPI_Comm comm, unsigned int bucket_size = 10,
			unsigned int global_deep = 2);
	virtual ~MPIDHashIndex();

	virtual bool synchronize();
	virtual Catalog* get_local_catalogs();
	virtual IndexHead* get_local_index();
	virtual unsigned int size_of_global_index();

	virtual unsigned int insert(const char *key, size_t key_length,
			unsigned int& key_info, unsigned int record_id);
	virtual unsigned int get_mark_record_num(const char *key,
			size_t key_length);
	virtual unsigned int get_real_record_num(const char *key,
			size_t key_length);
	virtual unsigned int find_record(unsigned int *records, const char *key,
			size_t key_length);
	virtual bool get_key_info(unsigned int& key_info, const char *key,
			size_t key_length);
	virtual unsigned int* get_intersect_records(const char **keys,
			unsigned int key_num);

private:
	/*
	 * description: 生成统计列表函数
	 *  parameters:
	 *      return: 在每个目录项里平均存有的索引项个数
	 */
	unsigned int* gen_statistics();
	/*
	 * description: 为同步操作的Gather操作生成消息类型
	 *  parameters: (IN)global_deep:	动态哈希表的全局深度
	 *  			(IN)statistics:		统计列表
	 *  			(OUT)datatype:		MPI数据类型
	 *      return: 生成是否成功
	 */
	bool gen_syng_msg_type(unsigned int global_deep, unsigned int* statistics,
			MPI_Datatype& newtype, SynGatherMsg& msg);
	/*
	 * description: 为同步操作的Gather操作生成消息类型
	 *  parameters: (IN)global_deep:	动态哈希表的全局深度
	 *  			(IN)statistics:		统计列表
	 *  			(OUT)datatype:		MPI数据类型
	 *      return: 生成是否成功
	 */
	bool gen_synb_msg_type(unsigned int numprocs, MPI_Datatype& newtype,
			SynBcastMsg& msg);
	/*
	 * description: 合并桶函数
	 *  parameters: buckets:	桶列表
	 *  			bucket_num:	桶数量
	 *      return: 合并的桶
	 */
	Bucket* union_bucket(Bucket* buckets, unsigned int bucket_num);

protected:
	pair<unsigned int, unsigned int> m_responsible_cats; //负责的目录（起始索引，长度）

private:
	MPI_Comm m_comm;
	int m_root_pid;
	volatile bool is_synchronized;
};

#endif /* MPI_D_HASH_INDEX_H_ */
