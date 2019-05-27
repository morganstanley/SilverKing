package com.ms.silverking.cloud.ring;

/**
 * disjoint:
 *                  aaaaaa
 *                          bbbbbb
 * isomorphic:
 *                  aaaaaa
 *                  bbbbbb
 * abPartial:
 *                  aaaaaa
 *                     bbbbbb
 * baPartial:
 *                     aaaaaa
 *                  bbbbbb
 * aSubsumesB:
 *            aaaaaa  aaaaaaaa  aaaaaa 
 *            bbbb      bbbb      bbbb
 * bSubsumesA:
 *            aaaa      aaaa      aaaa
 *            bbbbbb  bbbbbbbb  bbbbbb
 * wrappedPartial:
 *       aaaa  aaaa     aaaaaa
 *         bbbbbb     bbbb  bbbb
 *    (due to wrapping, a contains both the start and end of b and vice versa)                  
 *                
 * nonIdenticalAllRingspace:
 * 				  a&b both contain the entire ringspace, but have distinct starts and ends
 *                We currently prevent this by normalizing all entire ringspace RingRegions
 *                This type is included for completeness
 */
public enum IntersectionType {disjoint, isomorphic, abPartial, baPartial, aSubsumesB, bSubsumesA, wrappedPartial, nonIdenticalAllRingspace}