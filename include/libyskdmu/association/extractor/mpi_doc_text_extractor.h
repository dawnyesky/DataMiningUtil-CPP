/*
 * mpi_doc_text_extractor.h
 *
 *  Created on: 2013-8-5
 *      Author: Yan Shankai
 */

#ifndef MPI_DOC_TEXT_EXTRACTOR_H_
#define MPI_DOC_TEXT_EXTRACTOR_H_

#include <mpi.h>
#include "libyskdmu/association/extractor/doc_text_extractor.h"

class MPIDocTextExtractor: public DocTextExtractor {
public:
	MPIDocTextExtractor(MPI_Comm comm, int root_pid = 0);
	MPIDocTextExtractor(vector<DocTextRecordInfo>* record_infos,
			vector<vector<DocItem> >* items,
			vector<DocItemDetail>* item_details, HashIndex* item_index,
			MPI_Comm comm, int root_pid = 0);
	virtual ~MPIDocTextExtractor();
	void read_data(bool with_hi);
	bool hi_extract_record(void* data_addr);

private:
	pair<void*, int*> pack_scafile_msg(vector<vector<char*> >& v_files);
	pair<vector<char*>*, int> unpack_scafile_msg(pair<void*, int> msg_pkg);

public:
	unsigned int m_record_num;
	unsigned int m_record_size;

private:
	MPI_Comm m_comm;
	int m_root_pid;
	unsigned int m_record_offset;
	unsigned int m_item_detail_offset;
	unsigned int SCAFILE_BUF_SIZE;
};

#endif /* MPI_DOC_TEXT_EXTRACTOR_H_ */
