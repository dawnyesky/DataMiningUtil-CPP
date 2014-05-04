/*
 * global.h
 *
 *  Created on: 2013-10-10
 *      Author: Yan Shankai
 */

#ifndef GLOBAL_H_
#define GLOBAL_H_

#ifdef OMP
#define CLASSDECL __attribute__((target(mic:0)))
#else
#define CLASSDECL
#endif

#ifdef OMP
#define FUNCDECL __attribute__((target(mic:0)))
#else
#define FUNCDECL
#endif

#ifdef OMP
#define PROPDECL __attribute__((target(mic:0)))
#else
#define PROPDECL
#endif

#ifdef OMP
#define ALLOC alloc_if(1) free_if(0)
#define FREE length(0) alloc_if(0) free_if(1)
#define REUSE length(0) alloc_if(0) free_if(0)
#endif

#endif /* GLOBAL_H_ */
