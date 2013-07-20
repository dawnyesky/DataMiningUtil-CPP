/*
 * mpi_d_hash_index.cpp
 *
 *  Created on: 2013-7-10
 *      Author: Yan Shankai
 */

#include <string.h>
#include <math.h>
#include <algorithm>
#include "libyskdmu/index/mpi_d_hash_index.h"

MPIDHashIndex::MPIDHashIndex(MPI_Comm comm, unsigned int bucket_size,
		unsigned int global_deep) :
		DynamicHashIndex(bucket_size, global_deep) {
	m_comm = comm;
//	m_bucket_size = bucket_size;
//	m_d = global_deep;
	m_root_pid = 0;
	m_responsible_cats.first = 0;
	m_responsible_cats.second = 0;
	m_log_fp = LogUtil::get_instance()->get_log_instance("mpiDHashIndex");
	is_synchronized = false;
}

MPIDHashIndex::~MPIDHashIndex() {

}

bool MPIDHashIndex::synchronize() {
	int pid, numprocs;
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);

	//声明和准备Gather消息数据
	SynGatherMsg syng_send_msg;
	syng_send_msg.global_deep = m_d;
	syng_send_msg.statistics = gen_statistics();
	pair<void*, int> syng_recv_msg_pkg;
	syng_recv_msg_pkg.first = NULL;
	syng_recv_msg_pkg.second = 0;
	if (pid == m_root_pid) {
		syng_recv_msg_pkg.first = malloc(numprocs * SYNG_RECV_BUF_SIZE);
		syng_recv_msg_pkg.second = SYNG_RECV_BUF_SIZE;
	}

	//打包Gather消息
	pair<void*, int> syng_send_msg_pkg = pack_syng_msg(syng_send_msg);

	//汇总
	MPI_Barrier(m_comm);
	MPI_Gather(syng_send_msg_pkg.first, syng_send_msg_pkg.second, MPI_PACKED,
			syng_recv_msg_pkg.first, syng_recv_msg_pkg.second, MPI_PACKED,
			m_root_pid, m_comm);

	//解包Gather消息
	SynGatherMsg* syng_recv_msg = unpack_syng_msg(syng_recv_msg_pkg);

	//声明和准备Broadcast消息数据
	SynBcastMsg synb_msg;
	synb_msg.global_deep = m_d;
	synb_msg.catalog_offset = new unsigned int[numprocs];
	synb_msg.catalog_size = new unsigned int[numprocs];

	//处理汇总数据
	if (pid == m_root_pid) {
		//找出最大全局深度
		unsigned int max_pid = 0;
		unsigned int max_d = syng_recv_msg[0].global_deep;
		for (unsigned int i = 1; i < numprocs; i++) {
			if (syng_recv_msg[i].global_deep > max_d) {
				max_pid = syng_recv_msg[i].global_deep;
				max_d = i;
			}
		}

		//统计
		unsigned int catalog_size = pow(2, max_d);
		unsigned int counter[catalog_size];
		for (unsigned int i = 0; i < numprocs; i++) {
			//把所有节点的目录都展开到最大全局深度
			if (syng_recv_msg[i].global_deep < max_d) {
				unsigned int old_catalog_size = pow(2,
						syng_recv_msg[i].global_deep);
				for (unsigned int j = 0; j < old_catalog_size; j++) {
					unsigned int multiple = catalog_size / old_catalog_size;
					for (unsigned int k = 0; k < multiple; k++) {
						unsigned int cid = j + k * old_catalog_size;
						counter[cid] = syng_recv_msg[i].statistics[j]
								/ multiple;
					}
				}
			}
		}

		unsigned int sum = 0; //总和
		for (unsigned int i = 0; i < catalog_size; i++) {
			sum += counter[i];
		}
		unsigned int element_per_proc = sum / numprocs; //平均每个节点处理的索引项

		//准备Broadcast消息数据
		synb_msg.global_deep = max_d;
		synb_msg.catalog_offset[0] = 0;
		for (unsigned int i = 0, pid = 0, accumulation = 0; i < catalog_size;
				i++) {
			accumulation += counter[i];
			if (accumulation < element_per_proc) {
				continue;
			}
			synb_msg.catalog_size[pid] = i - synb_msg.catalog_offset[pid] + 1;
			accumulation = 0;
			pid++;
			if (pid < numprocs) {
				synb_msg.catalog_offset[pid] = i + 1;
			}
		}
		//调整分配末尾的几个节点，负载均衡
		if (synb_msg.catalog_size[numprocs - 1] == 0) {
			unsigned int last_pid = numprocs - 1;
			unsigned int accumulation = synb_msg.catalog_size[last_pid];
			while (accumulation < numprocs - last_pid) {
				last_pid--;
				accumulation += synb_msg.catalog_size[last_pid];
			}
			unsigned int offset = synb_msg.catalog_offset[last_pid];
			for (unsigned int i = last_pid; i < numprocs; i++, offset++) {
				synb_msg.catalog_offset[i] = offset;
				synb_msg.catalog_size[i] = 1;
			}
			//最后一个节点负责剩余的1～2个
			while (offset != catalog_size) {
				synb_msg.catalog_size[numprocs - 1]++;
				offset++;
			}
		}
	}

	//打包Broadcast消息
	pair<void*, int> synb_msg_pkg = pack_synb_msg(synb_msg);

	//广播
	MPI_Bcast(synb_msg_pkg.first, synb_msg_pkg.second, MPI_PACKED, m_root_pid,
			m_comm);

	if (pid != m_root_pid) {
		delete[] synb_msg.catalog_offset;
		delete[] synb_msg.catalog_size;
		SynBcastMsg* bcast_msg = unpack_synb_msg(synb_msg_pkg);
		synb_msg.catalog_offset = bcast_msg->catalog_offset;
		synb_msg.catalog_size = bcast_msg->catalog_size;
		delete bcast_msg;
	}

	//处理统计数据
	m_responsible_cats.first = synb_msg.catalog_offset[pid];
	m_responsible_cats.second = synb_msg.catalog_size[pid];
	split_catalog(synb_msg.global_deep);
	for (unsigned int i = 0; i < (unsigned int) pow(2, m_d); i++) {
		if (m_catalogs[i].l < m_d) {
			split_bucket(i, m_d);
		}
	}

	//准备Alltoall发送的数据
	unsigned int catalogs_size = pow(2, m_d);
	Bucket send_buckets[catalogs_size];
	for (unsigned int i = 0; i < catalogs_size; i++) {
		send_buckets[i] = *m_catalogs[i].bucket;
	}
	SynAlltoallMsg synata_send_msg;
	synata_send_msg.buckets = send_buckets;
	synata_send_msg.bucket_num = catalogs_size;
	int send_buf_offset[numprocs];
	int send_buf_size[numprocs];
	//打包Alltoall消息
	pair<void*, int*> synata_send_msg_pkg = pack_synata_msg(synata_send_msg,
			synb_msg.catalog_offset, synb_msg.catalog_size);

	//准备Alltoall接受的数据
	int recv_buf_offset[numprocs];
	int recv_buf_size[numprocs];
	for (unsigned int i = 0; i < numprocs; i++) {
		recv_buf_offset[i] = i * SYNATA_RECV_BUF_SIZE;
		recv_buf_size[i] = SYNATA_RECV_BUF_SIZE;
	}
	pair<void*, int> synata_recv_msg_pkg;
	synata_recv_msg_pkg.first = malloc(numprocs * SYNATA_RECV_BUF_SIZE);
	synata_recv_msg_pkg.second = numprocs * SYNATA_RECV_BUF_SIZE;

	//交换数据
	MPI_Alltoallv(synata_send_msg_pkg.first, send_buf_size, send_buf_offset,
			MPI_PACKED, synata_recv_msg_pkg.first, recv_buf_size,
			recv_buf_offset, MPI_PACKED, m_comm);

	//解包Alltoall消息，存储的是已合并各个进程里属于自己负责的桶列表
	SynAlltoallMsg* synata_recv_msg = unpack_synata_msg(synata_recv_msg_pkg);

//	Bucket* recv_buckets[numprocs];
//	for (unsigned int i = 0; i < numprocs; i++) {
//		recv_buckets[i] = synata_recv_msg[i].buckets;
//	}
	assert(synata_recv_msg->bucket_num == m_responsible_cats.second);
	//处理交换数据
	for (unsigned int i = m_responsible_cats.first;
			i < m_responsible_cats.second; i++) {
		if (m_catalogs[i].bucket != NULL) {
			delete m_catalogs[i].bucket;
		}
		m_catalogs[i].bucket = new Bucket;
		m_catalogs[i].bucket->elements = synata_recv_msg->buckets[i
				- m_responsible_cats.first].elements;
	}
//	for (unsigned int i = m_responsible_cats.first;
//			i < m_responsible_cats.second; i++) {
//		Bucket buckets[numprocs];
//		for (unsigned int j = 0; j < numprocs; j++) {
//			buckets[j] = recv_buckets[j][i];
//		}
//		if (m_catalogs[i].bucket != NULL) {
//			delete m_catalogs[i].bucket;
//		}
//		m_catalogs[i].bucket = new Bucket;
//		union_bucket(m_catalogs[i].bucket, buckets, numprocs);
//	}
	is_synchronized = true;

	if (pid == m_root_pid) {
		for (unsigned int i = 0; i < numprocs; i++) {
			delete[] syng_recv_msg[i].statistics;
		}
		delete[] syng_recv_msg;
		free(syng_recv_msg_pkg.first);
	}
	delete[] syng_send_msg.statistics;
	delete[] synb_msg.catalog_offset;
	delete[] synb_msg.catalog_size;
	for (unsigned int i = 0; i < numprocs; i++) {
		delete synata_recv_msg[i].buckets;
	}
	free(syng_send_msg_pkg.first);
	free(synb_msg_pkg.first);
	free(synata_send_msg_pkg.first);
	free(synata_recv_msg_pkg.first);
	return true;
}

Catalog* MPIDHashIndex::get_local_catalogs() {
	Catalog* result = new Catalog[m_responsible_cats.second];
	for (unsigned int i = 0; i < m_responsible_cats.second; i++) {
		result[i].l = m_catalogs[i + m_responsible_cats.first].l;
		result[i].bucket = NULL;
	}
	return result;
}

IndexHead* MPIDHashIndex::get_local_index() {
	Bucket* buckets[m_responsible_cats.second];
	unsigned int elements_sum = 0;
	for (unsigned int i = 0; i < m_responsible_cats.second; i++) {
		buckets[i] = m_catalogs[i + m_responsible_cats.first].bucket;
		elements_sum += buckets[i]->elements.size();
	}
	IndexHead* result = new IndexHead[elements_sum];
	for (unsigned int i = 0, index = 0; i < m_responsible_cats.second; i++) {
		for (unsigned int j = 0; j < buckets[i]->elements.size(); j++) {
			result[index] = buckets[i]->elements[j];
			index++;
		}
	}
	return result;
}

unsigned int MPIDHashIndex::size_of_global_index() {
	return 0;
}

unsigned int MPIDHashIndex::insert(const char *key, size_t key_length,
		unsigned int& key_info, unsigned int record_id) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	if (!is_synchronized
			|| (catalog_id >= m_responsible_cats.first
					&& catalog_id
							< m_responsible_cats.first
									+ m_responsible_cats.second)) { //本地访问
		return DynamicHashIndex::insert(key, key_length, key_info, record_id);
	} else { //远程访问
		return 0;
	}
}

unsigned int MPIDHashIndex::get_mark_record_num(const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	if (catalog_id >= m_responsible_cats.first
			&& catalog_id
					< m_responsible_cats.first + m_responsible_cats.second) { //本地访问
		return DynamicHashIndex::get_mark_record_num(key, key_length);
	} else { //远程访问
		return 0;
	}
}

unsigned int MPIDHashIndex::get_real_record_num(const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	if (catalog_id >= m_responsible_cats.first
			&& catalog_id
					< m_responsible_cats.first + m_responsible_cats.second) { //本地访问
		return DynamicHashIndex::get_real_record_num(key, key_length);
	} else { //远程访问
		return 0;
	}
}

unsigned int MPIDHashIndex::find_record(unsigned int *records, const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	if (catalog_id >= m_responsible_cats.first
			&& catalog_id
					< m_responsible_cats.first + m_responsible_cats.second) { //本地访问
		return DynamicHashIndex::find_record(records, key, key_length);
	} else { //远程访问
		return 0;
	}
}

bool MPIDHashIndex::get_key_info(unsigned int& key_info, const char *key,
		size_t key_length) {
	unsigned int hashcode = hashfunc(key, key_length);
	unsigned int catalog_id = addressing(hashcode);
	if (catalog_id >= m_responsible_cats.first
			&& catalog_id
					< m_responsible_cats.first + m_responsible_cats.second) { //本地访问
		return DynamicHashIndex::get_key_info(key_info, key, key_length);
	} else { //远程访问
		return false;
	}
}

unsigned int* MPIDHashIndex::get_intersect_records(const char **keys,
		unsigned int key_num) {
	bool local_accessible = true;
	bool local_accessibles[key_num];
	for (unsigned int i = 0; i < key_num; i++) {
		unsigned int hashcode = hashfunc(keys[i], strlen(keys[i]));
		unsigned int catalog_id = addressing(hashcode);
		local_accessibles[i] = catalog_id >= m_responsible_cats.first
				&& catalog_id
						< m_responsible_cats.first + m_responsible_cats.second;
		local_accessible &= local_accessibles[i];
	}
	if (local_accessible) { //本地访问
		return DynamicHashIndex::get_intersect_records(keys, key_num);
	} else { //远程访问
		return NULL;
	}
}

unsigned int* MPIDHashIndex::gen_statistics() {
	unsigned int catalogs_size = pow(2, m_d);
	unsigned int* result = new unsigned int[catalogs_size];
	unsigned int handled[catalogs_size]; //标记是否之前遍历指向共同的桶的目录时已处理过
	memset(handled, 0, catalogs_size);
	for (unsigned int i = 0; i < catalogs_size; i++) {
		if (handled[i] == 0) {
			//如果有多个目录指向同一个桶则把平均值记入各个目录里
			unsigned int element_avg_num = m_catalogs[i].bucket->elements.size()
					/ (unsigned int) pow(2, m_d - m_catalogs[i].l);
			for (unsigned int j = 0; j < (int) pow(2, m_d - m_catalogs[i].l);
					j++) {
				unsigned int cid = i
						+ j * (unsigned int) pow(2, m_catalogs[i].l);
				result[cid] = element_avg_num;
				handled[cid] = true;
			}
		}
	}
	return result;
}

pair<void*, int> MPIDHashIndex::pack_syng_msg(SynGatherMsg& msg) {
	pair<void*, int> result;
	unsigned int catalogs_size = pow(2, msg.global_deep);

	//计算缓冲区大小：
	result.second = 0;
	MPI_Pack_size(1 + catalogs_size, MPI_UNSIGNED, m_comm, &result.second);

	//分配缓冲区空间
	result.first = malloc(result.second);

	//开始打包
	int position = 0;
	MPI_Pack(&msg.global_deep, 1, MPI_UNSIGNED, result.first, result.second,
			&position, m_comm);
	MPI_Pack(msg.statistics, catalogs_size, MPI_UNSIGNED, result.first,
			result.second, &position, m_comm);

	return result;
}

SynGatherMsg* MPIDHashIndex::unpack_syng_msg(pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	SynGatherMsg* result = new SynGatherMsg[numprocs];
	for (unsigned int i = 0; i < numprocs; i++) {
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position,
				&result[i].global_deep, 1, MPI_UNSIGNED, m_comm);
		unsigned int catalog_size = pow(2, result[i].global_deep);
		result[i].statistics = new unsigned int[catalog_size];
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position,
				result[i].statistics, catalog_size, MPI_UNSIGNED, m_comm);
	}
	return result;
}

pair<void*, int> MPIDHashIndex::pack_synb_msg(SynBcastMsg& msg) {
	pair<void*, int> result;
	unsigned int catalogs_size = pow(2, msg.global_deep);

	//计算缓冲区大小：
	result.second = 0;
	MPI_Pack_size(1 + 2 * catalogs_size, MPI_UNSIGNED, m_comm, &result.second);

	//分配缓冲区空间
	result.first = malloc(result.second);

	if (msg.catalog_offset != NULL && msg.catalog_size != NULL) {
		//开始打包
		int position = 0;
		MPI_Pack(&msg.global_deep, 1, MPI_UNSIGNED, result.first, result.second,
				&position, m_comm);
		MPI_Pack(msg.catalog_offset, catalogs_size, MPI_UNSIGNED, result.first,
				result.second, &position, m_comm);
		MPI_Pack(msg.catalog_size, catalogs_size, MPI_UNSIGNED, result.first,
				result.second, &position, m_comm);
	}

	return result;
}

SynBcastMsg* MPIDHashIndex::unpack_synb_msg(pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	SynBcastMsg* result = new SynBcastMsg;
	for (unsigned int i = 0; i < numprocs; i++) {
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position,
				&result[i].global_deep, 1, MPI_UNSIGNED, m_comm);
		unsigned int catalog_size = pow(2, result[i].global_deep);
		result[i].catalog_offset = new unsigned int[catalog_size];
		result[i].catalog_size = new unsigned int[catalog_size];
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position,
				result[i].catalog_offset, catalog_size, MPI_UNSIGNED, m_comm);
		MPI_Unpack(msg_pkg.first, msg_pkg.second, &position,
				result[i].catalog_size, catalog_size, MPI_UNSIGNED, m_comm);
	}
	return result;
}

pair<void*, int*> MPIDHashIndex::pack_synata_msg(SynAlltoallMsg& msg,
		unsigned int* catalog_offset, unsigned int* catalog_size) {
	int numprocs;
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	pair<void*, int*> result;

	//计算缓冲区大小
	result.second = new int[numprocs];
	unsigned int total_fixed_uint_num = 0;
	unsigned int total_dynamic_uint_num = 0;
	unsigned int total_dynamic_char_num = 0;
	for (unsigned int i = 0; i < numprocs; i++) {
		int fixed_uint_num = 1 + catalog_size[i]; //每个进程的桶数量+每个桶里的索引数量
		int dynamic_uint_num = 0;
		int dynamic_char_num = 0;
		for (unsigned int j = 0; j < catalog_size[i]; j++) {
			unsigned int bid = catalog_offset[i] + j;
			fixed_uint_num += 3 * msg.buckets[bid].elements.size();
			for (vector<IndexHead>::iterator iter =
					msg.buckets[bid].elements.begin();
					iter != msg.buckets[bid].elements.end(); iter++) {
				dynamic_char_num += strlen(iter->identifier) + 1;
				dynamic_uint_num += iter->index_item_num;
			}
		}
		total_fixed_uint_num += fixed_uint_num;
		total_dynamic_uint_num += dynamic_uint_num;
		total_dynamic_char_num += dynamic_char_num;
		int fixed_uint_size, dynamic_uint_size, dynamic_char_size;
		MPI_Pack_size(fixed_uint_num, MPI_UNSIGNED, m_comm, &fixed_uint_size);
		MPI_Pack_size(dynamic_uint_num, MPI_UNSIGNED, m_comm,
				&dynamic_uint_size);
		MPI_Pack_size(dynamic_char_num, MPI_UNSIGNED, m_comm,
				&dynamic_char_size);
		result.second[i] = fixed_uint_size + dynamic_uint_size
				+ dynamic_char_size;
	}
//	fixed_uint_num += numprocs + msg.bucket_num; //每个进程的桶数量+每个桶里的索引数量
//	for (unsigned int i = 0; i < msg.bucket_num; i++) {
//		fixed_uint_num += 3 * msg.buckets[i].elements.size();
//		for (vector<IndexHead>::const_iterator iter =
//				msg.buckets[i].elements.begin();
//				iter != msg.buckets[i].elements.end(); iter++) {
//			dynamic_char_num += strlen(iter->identifier) + 1;
//			dynamic_uint_num += iter->index_item_num;
//		}
//	}
	int total_fixed_uint_size, total_dynamic_uint_size, total_dynamic_char_size;
	MPI_Pack_size(total_fixed_uint_num, MPI_UNSIGNED, m_comm,
			&total_fixed_uint_size);
	MPI_Pack_size(total_dynamic_uint_num, MPI_UNSIGNED, m_comm,
			&total_dynamic_uint_size);
	MPI_Pack_size(total_dynamic_char_num, MPI_CHAR, m_comm,
			&total_dynamic_char_size);
	int total_size = total_fixed_uint_size + total_dynamic_uint_size
			+ total_dynamic_char_size;

	//分配缓冲区空间
	result.first = malloc(total_size);

	//开始打包
	int position = 0;
	for (unsigned int i = 0; i < numprocs; i++) {
		MPI_Pack(catalog_size + i, 1, MPI_UNSIGNED, result.first, total_size,
				&position, m_comm); //每个进程的桶数量
		for (unsigned int j = 0; j < catalog_size[i]; j++) {
			unsigned int bid = catalog_offset[i] + j;
			unsigned int element_num = msg.buckets[bid].elements.size();
			MPI_Pack(&element_num, 1, MPI_UNSIGNED, result.first, total_size,
					&position, m_comm); //每个桶里的索引数量
			for (vector<IndexHead>::iterator iter =
					msg.buckets[bid].elements.begin();
					iter != msg.buckets[bid].elements.end(); iter++) {
				unsigned int id_len = strlen(iter->identifier) + 1;
				MPI_Pack(&id_len, 1, MPI_UNSIGNED, result.first, total_size,
						&position, m_comm); //索引标识长度
				MPI_Pack(iter->identifier, id_len, MPI_CHAR, result.first,
						total_size, &position, m_comm); //索引标识
				MPI_Pack(&iter->key_info, 1, MPI_UNSIGNED, result.first,
						total_size, &position, m_comm); //索引标识相关信息
				MPI_Pack(&iter->index_item_num, 1, MPI_UNSIGNED, result.first,
						total_size, &position, m_comm); //索引项数量
				IndexItem* p = iter->inverted_index;
				while (p != NULL) {
					MPI_Pack(&p->record_id, 1, MPI_UNSIGNED, result.first,
							total_size, &position, m_comm); //索引项
					p = p->next;
				}
			}
		}
	}
	return result;
}

SynAlltoallMsg* MPIDHashIndex::unpack_synata_msg(pair<void*, int> msg_pkg) {
	int numprocs, position = 0;
	unsigned int total_size = 0;
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	SynAlltoallMsg* result = new SynAlltoallMsg;
	//计算缓冲区总大小
	total_size += numprocs * msg_pkg.second;
	//分配结果桶空间
	MPI_Unpack(msg_pkg.first, total_size, &position, &result->bucket_num, 1,
			MPI_UNSIGNED, m_comm);
	result->buckets = new Bucket[result->bucket_num];
	Bucket buckets[result->bucket_num][numprocs];

	for (unsigned int i = 0; i < numprocs; i++) {
		position = i * SYNATA_RECV_BUF_SIZE;
		unsigned int bucket_num;
		MPI_Unpack(msg_pkg.first, total_size, &position, &bucket_num, 1,
				MPI_UNSIGNED, m_comm);
		assert(bucket_num == result->bucket_num);
		for (unsigned int j = 0; j < bucket_num; j++) {
			unsigned int element_num;
			MPI_Unpack(msg_pkg.first, total_size, &position, &element_num, 1,
					MPI_UNSIGNED, m_comm);
			for (unsigned int k = 0; k < element_num; k++) {
				IndexHead* index_head = new IndexHead();
				unsigned int id_len;
				MPI_Unpack(msg_pkg.first, total_size, &position, &id_len, 1,
						MPI_UNSIGNED, m_comm);
				index_head->identifier = new char[id_len];
				MPI_Unpack(msg_pkg.first, total_size, &position,
						index_head->identifier, id_len, MPI_CHAR, m_comm);
				MPI_Unpack(msg_pkg.first, total_size, &position,
						&index_head->key_info, id_len, MPI_CHAR, m_comm);
				MPI_Unpack(msg_pkg.first, total_size, &position,
						&index_head->index_item_num, id_len, MPI_CHAR, m_comm);
				IndexItem* p = NULL;
				for (unsigned int t = 0; t < index_head->index_item_num; t++) {
					IndexItem* index_item = new IndexItem();
					MPI_Unpack(msg_pkg.first, total_size, &position,
							&index_item->record_id, id_len, MPI_CHAR, m_comm);
					if (p == NULL) {
						index_head->inverted_index = index_item;
					} else {
						p->next = index_item;
					}
					index_item->next = NULL;
					p = index_item;
				}
				buckets[j][i].elements.push_back(*index_head);
			}
		}
	}

	//合并桶
	for (unsigned int i = 0; i < result->bucket_num; i++) {
		union_bucket(result->buckets + i, buckets[i], numprocs);
	}
	return result;
}

bool MPIDHashIndex::union_bucket(Bucket* out_bucket, Bucket* in_buckets,
		unsigned int bucket_num) {
	if (out_bucket == NULL) {
		out_bucket = new Bucket[bucket_num];
	}
	for (unsigned int i = 0; i < bucket_num; i++) {
		for (unsigned int j = 0; j < in_buckets[i].elements.size(); j++) {
			vector<IndexHead>::iterator iter = find(
					out_bucket[i].elements.begin(),
					out_bucket[i].elements.end(), in_buckets[i].elements[j]);
			if (iter == out_bucket[i].elements.end()) { //找不到索引项
				out_bucket[i].elements.push_back(*iter);
			} else {
				iter->index_item_num +=
						in_buckets[i].elements[j].index_item_num;
				IndexItem* p = iter->inverted_index;
				if (p == NULL) {
					iter->inverted_index =
							in_buckets[i].elements[j].inverted_index;
				} else {
					IndexItem* prv = NULL;
					while (p->record_id
							>= in_buckets[i].elements[j].inverted_index->record_id) {
						prv = p;
						p = p->next;
					}
					IndexItem* tail = NULL;
					if (prv == NULL) { //放在开始位置
						iter->inverted_index =
								in_buckets[i].elements[j].inverted_index;
						tail = iter->inverted_index;
					} else {
						prv->next = in_buckets[i].elements[j].inverted_index;
						tail = prv->next;
					}
					while (tail->next != NULL) {
						tail = tail->next;
					}
					tail->next = p;
				}
			}
		}
	}
	return true;
}
