/* Copyright (C) 2002 Christopher Clark <firstname.lastname@cl.cam.ac.uk> */

#ifndef __HASHTABLE_CWC22_UTILITY_H__
#define __HASHTABLE_CWC22_UTILITY_H__

/*****************************************************************************
 * hashtable_change
 *
 * function to change the value associated with a key, where there already
 * exists a value bound to the key in the hashtable.
 * Source due to Holger Schemel.
 *
 * @name        hashtable_change
 * @param   h   the hashtable
 * @param       key
 * @param       value
 *
 */
int
hashtable_change(struct hashtable *h, void *k, void *v);

#endif /* __HASHTABLE_CWC22_H__ */

