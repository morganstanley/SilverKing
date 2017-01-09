/*
 * C Hashtable Implementation by Christopher Clark
 * http://www.cl.cam.ac.uk/~cwc22/hashtable/
 *
 * Copyright (c) 2002, 2004, Christopher Clark
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of the original author; nor the names of any contributors
 * may be used to endorse or promote products derived from this software
 * without specific prior written permission.
 *
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * smdb string hash function
 * http://www.cse.yorku.ca/~oz/hash.html
 */


Source code for a hash table data structure in C

This code is made available under the terms of the new BSD license.

If you use this code, drop me an email. It's nice to feel useful occasionally.
I promise not to sell your email address to Nigerian spam bandits. Thanks.
Christopher Clark, January 2005.
Defined functions

    * create_hashtable
    * hashtable_insert
    * hashtable_search
    * hashtable_remove
    * hashtable_count
    * hashtable_destroy

Example of use

      struct hashtable  *h;
      struct some_key   *k;
      struct some_value *v;

      static unsigned int         hash_from_key_fn( void *k );
      static int                  keys_equal_fn ( void *key1, void *key2 );

      h = create_hashtable(16, hash_from_key_fn, keys_equal_fn);

      insert_key   = (struct some_key *) malloc(sizeof(struct some_key));
      retrieve_key = (struct some_key *) malloc(sizeof(struct some_key));

      v = (struct some_value *) malloc(sizeof(struct some_value));

      (You should initialise insert_key, retrieve_key and v here)

      if (! hashtable_insert(h,insert_key,v) )
      {     exit(-1);               }

      if (NULL == (found = hashtable_search(h,retrieve_key) ))
      {    printf("not found!");                  }

      if (NULL == (found = hashtable_remove(h,retrieve_key) ))
      {    printf("Not found\n");                 }

      hashtable_destroy(h,1); /* second arg indicates "free(value)" */

Description

The table will increase in size as elements are added, to keep the ratio of elements to table size below a threshold. The table is sized by selecting a prime number of appropriate magnitude, to ensure best distribution of the contents.

For improved type safety, macros have been defined and may be used to define type-safe(r) hashtable access functions, with methods specialized to take known key and value types as parameters. Example: Insert this at the start of your file:

 DEFINE_HASHTABLE_INSERT(insert_some, struct some_key, struct some_value);
 DEFINE_HASHTABLE_SEARCH(search_some, struct some_key, struct some_value);
 DEFINE_HASHTABLE_REMOVE(remove_some, struct some_key, struct some_value);

This defines the functions 'insert_some', 'search_some' and 'remove_some'. These operate just like hashtable_insert etc., with the same parameters, but their function signatures have 'struct some_key *' rather than 'void *', and hence can generate compile time errors if your program is supplying incorrect data as a key (and similarly for value).

Note that the hash and key equality functions passed to create_hashtable still take 'void *' parameters instead of 'some key *'. This shouldn't be a serious issue as they're only defined and passed once, and the other functions will ensure that only valid keys are supplied to them.

The cost for this checking is increased code size and runtime overhead - if performance is important, it may be worth switching back to the unsafe methods once your program has been debugged with the safe methods.
Iterator

The iterator is a simple one-way iterator over the hashtable contents, providing accessors for the the key and value at the current element.

    /* Iterator constructor only returns a valid iterator if
     * the hashtable is not empty */

    if (hashtable_count(h) > 0)
    {
        itr = hashtable_iterator(h);
        do {
            k = hashtable_iterator_key(itr);
            v = hashtable_iterator_value(itr);

            /* here (k,v) are a valid (key, value) pair */
            /* We could call 'hashtable_remove(h,k)' - and this operation
             * 'free's k and returns v.
             * However, after the operation, the iterator is broken.
             */

        } while (hashtable_iterator_advance(itr));
    }
    free(itr);


Notes

If hashing strings, remember that strcmp is not a boolean comparison function directly suitable for keys_equal_fn.

Archived copy of the original hashtable implementation, where table size is a power of two, rather than prime. [ hashtable_powers.c ]


C Hash Table

Credits

Thanks to Glenn Lawyer for pointing out a Makefile fix for the tester example code.

Thanks to Holger Schemel for reminding me to actually free the hashtable in the destroy function...
Also for the hashtable_change function, now found in the hashtable utility source.

Thanks to John Muehlhausen for the iterator bug report - fixed 2003-03-19.

Thanks to Leonid Nilva for feedback.

Thanks to Mark Seneski for thoroughly reviewing my code, catching some of my slips from best practice, and prompting me to finally get around to sizing the table using prime numbers.
Thanks due to Aaron Krowne for his table of prime numbers used to do this.
http://planetmath.org/encyclopedia/GoodHashTablePrimes.html

Thanks to A. B. M. Omar Faruk for spotting the bug in the sample code on the web page, where the same key struct was used for insertion into the hashtable, and then also for retrieval. Ownership of the key is claimed when it is inserted.

Christopher Clark
Updated 29th April, 2007.
