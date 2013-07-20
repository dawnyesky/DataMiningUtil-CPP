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
	unsigned int global_deep; //动态哈希表的全局深度
	unsigned int* statistics; //统计列表
};

struct SynBcastMsg {
	unsigned int global_deep; //动态哈希表的全局深度
	unsigned int* catalog_offset; //各进程负责的目录索引起始位置
	unsigned int* catalog_size; //各进程负责的目录索引数量
};

struct SynAlltoallMsg {
	unsigned int bucket_num;
	Bucket* buckets; //哈希表中的桶列表
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
	 *  parameters: (IN)msg:		要打包的消息结构
	 *      return: (输出缓冲区，缓冲区大小)，如果缓冲区大小为0则打包失败
	 */
	pair<void*, int> pack_syng_msg(SynGatherMsg& msg);
	SynGatherMsg* unpack_syng_msg(pair<void*, int> msg_pkg);
	/*
	 * description: 为同步操作的Gather操作生成消息类型
	 *  parameters: (IN)msg:		要打包的消息结构
	 *      return: (输出缓冲区，缓冲区大小)，如果缓冲区大小为0则打包失败
	 */
	pair<void*, int> pack_synb_msg(SynBcastMsg& msg);
	SynBcastMsg* unpack_synb_msg(pair<void*, int> msg_pkg);
	/*
	 * description: 合并桶函数
	 *  parameters: buckets:	桶列表
	 *  			bucket_num:	桶数量
	 *      return: 合并的桶
	 */
	pair<void*, int*> pack_synata_msg(SynAlltoallMsg& msg,
			unsigned int* catalog_offset, unsigned int* catalog_size);
	SynAlltoallMsg* unpack_synata_msg(pair<void*, int> msg_pkg);
	bool union_bucket(Bucket* out_bucket, Bucket* in_buckets,
			unsigned int bucket_num);

protected:
	pair<unsigned int, unsigned int> m_responsible_cats; //负责的目录（起始索引，长度）

private:
	MPI_Comm m_comm;
	int m_root_pid;
	volatile bool is_synchronized;
	const static unsigned int SYNG_RECV_BUF_SIZE = 4096;
	const static unsigned int SYNATA_RECV_BUF_SIZE = 4096;
};

#endif /* MPI_D_HASH_INDEX_H_ */
