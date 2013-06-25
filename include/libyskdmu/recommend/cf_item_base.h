/*
 * cf_item_base.h
 *
 *  Created on: 2012-11-5
 *      Author: "Yan Shankai"
 */

#ifndef CF_ITEM_BASE_H_
#define CF_ITEM_BASE_H_

#include "libyskdmu/recommend/cf_base.h"

using namespace std;

class ItemBaseCF: public ColFiltering {
public:
	ItemBaseCF();
	ItemBaseCF(map<unsigned int, map<unsigned int, double>*>* data_matrix);
	virtual ~ItemBaseCF();
	bool init();
	void clear();
	map<unsigned int, map<unsigned int, double>*>* transform_prefs(
			map<unsigned int, map<unsigned int, double>*>* data_matrix);

	void calc_sim_items(
			map<unsigned int, map<unsigned int, double>*>* data_matrix);

	map<unsigned int, vector<unsigned int>*>* calc_sim_items(unsigned int k);

	vector<pair<unsigned int, double> >* get_recommendations(
			map<unsigned int, map<unsigned int, double>*>* data_matrix,
			unsigned int target_obj, unsigned int n);

	map<unsigned int, map<unsigned int, double>*>* calc_recommendation(
			map<unsigned int, map<unsigned int, double>*>* data_matrix,
			unsigned int n);

	map<unsigned int, map<unsigned int, double>*>* get_trans_matrix();

protected:
	map<unsigned int, vector<unsigned int>*>* m_sim_items;
	map<unsigned int, map<unsigned int, double>*>* m_trans_matrix;
	map<pair<unsigned int, unsigned int>, double> m_sim_matrix;
};

#endif /* CF_ITEM_BASE_H_ */
