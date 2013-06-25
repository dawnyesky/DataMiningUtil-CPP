/*
 * cf_user_base.h
 *
 *  Created on: 2012-10-30
 *      Author: "Yan Shankai"
 */

#ifndef CF_USER_BASE_H_
#define CF_USER_BASE_H_

#include "libyskdmu/recommend/cf_base.h"

using namespace std;

class UserBaseCF: public ColFiltering {
public:
	UserBaseCF();
	UserBaseCF(map<unsigned int, map<unsigned int, double>*>* data_matrix);
	virtual ~UserBaseCF();
	bool init();
	void clear();
	vector<pair<unsigned int, double> >* get_recommendations(
			map<unsigned int, map<unsigned int, double>*>* data_matrix,
			unsigned int target_obj, unsigned int n);

	map<unsigned int, map<unsigned int, double>*>* calc_recommendation(
			map<unsigned int, map<unsigned int, double>*>* data_matrix,
			unsigned int n);
};

#endif /* CF_USER_BASE_H_ */
