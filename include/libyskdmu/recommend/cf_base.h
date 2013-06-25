/*
 * cf_base.h
 *
 *  Created on: 2012-11-6
 *      Author: "Yan Shankai"
 */

#ifndef CF_BASE_H_
#define CF_BASE_H_

#include <vector>
#include <map>
#include "libyskdmu/cluster/similarity.h"

using namespace std;

class ColFiltering {
public:
	ColFiltering();
	ColFiltering(map<unsigned int, map<unsigned int, double>*>* data_matrix);
	virtual ~ColFiltering();
	virtual bool init() = 0;
	virtual void clear() = 0;
	vector<pair<unsigned int, double> >* top_matches(
			map<unsigned int, map<unsigned int, double>*>* data_matrix,
			unsigned int target_obj, unsigned int n);

	virtual vector<pair<unsigned int, double> >* get_recommendations(
			map<unsigned int, map<unsigned int, double>*>* data_matrix,
			unsigned int target_obj, unsigned int n) = 0;

	virtual map<unsigned int, map<unsigned int, double>*>* calc_recommendation(
			map<unsigned int, map<unsigned int, double>*>* data_matrix,
			unsigned int k) = 0;

	void set_data_matrix(
			map<unsigned int, map<unsigned int, double>*>* data_matrix);

	void set_sim_threshold(double sim_threshold);

	void set_sim_func(SimDistance similarity);

protected:
	map<unsigned int, map<unsigned int, double>*>* m_data_matrix;
	double m_sim_threshold;
	SimDistance m_similarity;
};

#endif /* CF_BASE_H_ */
