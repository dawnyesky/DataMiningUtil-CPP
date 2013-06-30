/*
 * doc_text_extractor.h
 *
 *  Created on: 2011-12-8
 *      Author: Yan Shankai
 */

#ifndef DOC_TEXT_EXTRACTOR_H_
#define DOC_TEXT_EXTRACTOR_H_

#include <vector>
#include "ICTCLAS50.h"
#include "libyskdmu/association/entity/doc_item.h"
#include "libyskdmu/association/extractor/extractor_interface.h"

using namespace std;

static const char ICTCLAS_INIT_DIR[] = "./shared/ICTCLAS";
static const char INPUT_DIR[] = "./shared/GoalFiles/";
static const eCodeType CODETYPE = CODE_TYPE_GB;

class DocTextExtractor: public Extractor<DocItem, DocItemDetail,
		DocTextRecordInfo> {
public:
	DocTextExtractor();
	DocTextExtractor(vector<DocTextRecordInfo>* record_infos,
			vector<vector<DocItem> >* items,
			vector<DocItemDetail>* item_details, HashIndex* item_index);
	virtual ~DocTextExtractor();
	void read_data(bool with_hi);
	bool extract_record(void* data_addr);
	bool hi_extract_record(void* data_addr);
};

#endif /* DOC_TEXT_EXTRACTOR_H_ */
