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
	unsigned int catalogs_size = pow(2, m_d);
	unsigned int* statistics = gen_statistics();

	//声明和准备Gather消息数据
	SynGatherMsg syng_send_msg;
	syng_send_msg.global_deep = m_d;
	syng_send_msg.statistics = statistics;
	SynGatherMsg* syng_recv_msg = NULL;
	//声明Broadcast消息数据
	SynBcastMsg synb_msg;
	synb_msg.global_deep = m_d;
	synb_msg.catalog_offset = new unsigned int[numprocs];
	synb_msg.catalog_size = new unsigned int[numprocs];
	if (pid == m_root_pid) {
		syng_recv_msg = new SynGatherMsg[numprocs];
	}
	//准备Gather消息数据类型
	MPI_Datatype syng_msg_type;
	gen_syng_msg_type(m_d, statistics, syng_msg_type, syng_send_msg);
	//准备Broadcast消息数据类型
	MPI_Datatype synb_msg_type;
	gen_synb_msg_type(m_d, synb_msg_type, synb_msg);
	MPI_Barrier(m_comm);
	//汇总
	MPI_Gather(&syng_send_msg, 1, syng_msg_type, &syng_recv_msg, 1,
			syng_msg_type, m_root_pid, m_comm);

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

	//广播
	MPI_Bcast(&synb_msg, 1, synb_msg_type, m_root_pid, m_comm);

	//处理统计数据
	m_responsible_cats.first = synb_msg.catalog_offset[pid];
	m_responsible_cats.second = synb_msg.catalog_size[pid];
	split_catalog(synb_msg.global_deep);
	for (unsigned int i = 0; i < (unsigned int) pow(2, m_d); i++) {
		if (m_catalogs[i].l < m_d) {
			split_bucket(i, m_d);
		}
	}

	//交换数据
	catalogs_size = pow(2, m_d);
	Bucket send_buckets[catalogs_size];
	Bucket recv_buckets[numprocs][m_responsible_cats.second];
	int recv_buf_offset[numprocs];
	int recv_buf_size[numprocs];
	for (unsigned int i = 0; i < numprocs; i++) {
		recv_buf_offset[i] = i * m_responsible_cats.second * sizeof(Bucket);
		recv_buf_size[i] = (int) m_responsible_cats.second;
	}
	for (unsigned int i = 0; i < catalogs_size; i++) {
		send_buckets[i] = *m_catalogs[i].bucket;
	}
	MPI_Alltoallv(send_buckets, (int*) synb_msg.catalog_size,
			(int*) synb_msg.catalog_offset, synb_msg_type, recv_buckets,
			recv_buf_size, recv_buf_offset, synb_msg_type, m_comm);

	//处理交换数据
	for (unsigned int i = m_responsible_cats.first;
			i < m_responsible_cats.second; i++) {
		Bucket buckets[numprocs];
		for (unsigned int j = 0; j < numprocs; j++) {
			buckets[j] = recv_buckets[j][i];
		}
		if (m_catalogs[i].bucket != NULL) {
			delete m_catalogs[i].bucket;
		}
		m_catalogs[i].bucket = union_bucket(buckets, numprocs);
	}
	is_synchronized = true;

	//释放消息数据类型
	MPI_Type_free(&syng_msg_type);
	MPI_Type_free(&synb_msg_type);
	if (pid == m_root_pid) {
		delete[] syng_recv_msg;
	}
	delete[] statistics;
	delete[] synb_msg.catalog_offset;
	delete[] synb_msg.catalog_size;
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
	unsigned int handled[catalogs_size];
	memset(handled, 0, catalogs_size);
	for (unsigned int i = 0; i < catalogs_size; i++) {
		if (handled[i] == 0) {
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

bool MPIDHashIndex::gen_syng_msg_type(unsigned int global_deep,
		unsigned int* statistics, MPI_Datatype& newtype, SynGatherMsg& msg) {
	int blocklens[2];
	MPI_Aint indices[2];
	MPI_Datatype old_type[2] = { MPI_UNSIGNED, MPI_UNSIGNED };

	blocklens[0] = 1;
	blocklens[1] = (unsigned int) pow(2, global_deep);

	MPI_Address(&msg.global_deep, &indices[0]);
	MPI_Address(&msg.statistics, &indices[1]);

	indices[1] = indices[1] - indices[0];
	indices[0] = 0;

	MPI_Type_struct(2, blocklens, indices, old_type, &newtype);
	MPI_Type_commit(&newtype);

	return true;
}

bool MPIDHashIndex::gen_synb_msg_type(unsigned int numprocs,
		MPI_Datatype& newtype, SynBcastMsg& msg) {
	int blocklens[3];
	MPI_Aint indices[3];
	MPI_Datatype old_type[3] = { MPI_UNSIGNED, MPI_UNSIGNED, MPI_UNSIGNED };

	blocklens[0] = 1;
	blocklens[1] = numprocs;
	blocklens[2] = numprocs;

	MPI_Address(&msg.global_deep, &indices[0]);
	MPI_Address(&msg.catalog_offset, &indices[1]);
	MPI_Address(&msg.catalog_size, &indices[2]);

	indices[2] = indices[2] - indices[1];
	indices[1] = indices[1] - indices[0];
	indices[0] = 0;

	MPI_Type_struct(3, blocklens, indices, old_type, &newtype);
	MPI_Type_commit(&newtype);

	return true;
}

Bucket* MPIDHashIndex::union_bucket(Bucket* buckets, unsigned int bucket_num) {
	Bucket* result = new Bucket[bucket_num];
	for (unsigned int i = 0; i < bucket_num; i++) {
		for (unsigned int j = 0; j < buckets[i].elements.size(); j++) {
			vector<IndexHead>::iterator iter = find(result[i].elements.begin(),
					result[i].elements.end(), buckets[i].elements[j]);
			if (iter == result[i].elements.end()) { //找不到索引项
				result[i].elements.push_back(*iter);
			} else {
				iter->index_item_num += buckets[i].elements[j].index_item_num;
				IndexItem* p = iter->inverted_index;
				if (p == NULL) {
					iter->inverted_index =
							buckets[i].elements[j].inverted_index;
				} else {
					IndexItem* prv = NULL;
					while (p->record_id
							>= buckets[i].elements[j].inverted_index->record_id) {
						prv = p;
						p = p->next;
					}
					IndexItem* tail = NULL;
					if (prv == NULL) { //放在开始位置
						iter->inverted_index =
								buckets[i].elements[j].inverted_index;
						tail = iter->inverted_index;
					} else {
						prv->next = buckets[i].elements[j].inverted_index;
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
	return result;
}
