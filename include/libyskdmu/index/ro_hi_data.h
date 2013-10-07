/*
 * ro_hi_data.h
 *
 *  Created on: 2013-10-11
 *      Author: Yan Shankai
 */

#ifndef RO_HI_DATA_H_
#define RO_HI_DATA_H_

#include <utility>
#include "libyskdmu/index/hash_index.h"

using namespace std;

class ROHashIndexData {
public:
	virtual ~ROHashIndexData() {
	}
	/*
	 * description: 获取索引
	 *      return: 索引数据
	 *      		索引所占内存大小，单位(byte)
	 */
	virtual pair<void*, unsigned int> get_index_data();
	bool is_built();

	/*
	 * description: 根据一个完整的索引构建只读索引
	 *      return: 构建是否成功
	 */
	virtual bool build(HashIndex* original_index) = 0;

protected:
	void* m_data; //索引数据块
	unsigned int m_data_size; //索引数据块大小
	HashFunc m_hash_func; //哈希函数指针
	bool m_is_built; //是否已经构建
};

#endif /* RO_HI_DATA_H_ */
