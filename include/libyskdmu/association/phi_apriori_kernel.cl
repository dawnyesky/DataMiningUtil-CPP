__kernel void decarl_intersect(__global long* result, __global uint* result_count, __global const ulong* data, __global const ulong* in_indice, __global const ulong* in_offset, __global const ulong* out_indice, __global const ulong* out_offset, __global const ulong* out_offset_indice, __global const ulong* task_indice, const ulong work_load) {
	const size_t id = get_global_id(0);
	if (id >= work_load) {
		return;
	}
	
	const ulong target = data[in_indice[id] + in_offset[id]];
	const ulong space_offset_length = out_offset[out_offset_indice[id]];
	__global ulong* space_offset = &out_offset[out_offset_indice[id] + 1];
	__global ulong* space = &data[out_indice[id]];
	
	ulong boundary[2];
	boundary[0] = 0;
	boundary[1] = space_offset_length - 1;
	while (boundary[0] + 1 > boundary[1]) {
		ulong mid = (boundary[0] + boundary[1]) / 2;
		if (space[space_offset[mid]] != target) {
			long diff = space[space_offset[mid]] - target;
			boundary[((diff >> 31) & 1)] = mid;
		} else {
			result[id] = space_offset[mid];
			atomic_add(&result_count[task_indice[id]], 1);
			return;
		}
	}
	for (ulong i = boundary[0]; i <= boundary[1]; i++) {
		if (space[space_offset[i]] == target) {
			result[id] = space_offset[i];
			atomic_add(&result_count[task_indice[id]], 1);
			return;
		}
	}
	
	result[id] = -1;
}