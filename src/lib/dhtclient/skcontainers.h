/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKCONTAINERS_H
#define SKCONTAINERS_H

#include <vector>
#include <string>
using std::string;
#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>
#include "SKStoredValue.h"

namespace jace { namespace proxy { namespace java { namespace util { 
        class Set;
        class Map;
        class Iterator;
} } } };
typedef jace::proxy::java::util::Set Set;
typedef jace::proxy::java::util::Map Map;
typedef jace::proxy::java::util::Iterator Iterator;

template<typename T> class SKVector
{
public:
  void push_back( const T & value );
  void pop_back ( void );
  T const & at        ( unsigned int index ) const;
  unsigned int size ( void ) const;
  bool empty    ( void ) const;
  void clear    ( void );
private:
  std::vector<T> implVec;
};


//********** Vector *************//
template<typename T>
void SKVector<T>::push_back(const T & v){
    return implVec.push_back(v);
}

template<typename T>
T const & SKVector<T>::at(unsigned int i) const {
    return implVec.at(i);
}

template<typename T>
unsigned int SKVector<T>::size ( void ) const {
    return implVec.size();
}

template<typename T>
bool SKVector<T>::empty ( void ) const {
    return implVec.empty();
}

template<typename T>
void SKVector<T>::clear ( void ) {
    return implVec.clear();
}

template<typename T>
void SKVector<T>::pop_back( void ){
    return implVec.pop_back();
}


/*
#ifndef _MSC_VER
template <
  typename K,
  typename V,
  typename Hash = std::hash<K>,
  typename KeyEqual = std::equal_to<K>,
  class Allocator = std::allocator< std::pair<const K, V> >
>
using SKMap = std::unordered_map<K,V>;
//using SKMap = std::unordered_map<K,V,Hash,KeyEqual,Allocator>;
#else
*/
    #define SKMap boost::unordered_map
/*
#endif  //_MSC_VER
*/

template<typename T>
struct Allowed; // undefined for bad types!
template<> struct Allowed<SKVal*> { };
template<> struct Allowed<SKStoredValue*> { };
//TODO ? :
//template<> struct Allowed<int> { };
//template<> struct Allowed<int64_t> { };
//template<> struct Allowed<double> { };

template<typename T>
class JSKMap  : private Allowed<T>
{
public:

    class iterator {
    public:
        SKAPI ~iterator();
        SKAPI std::pair<std::string, T >* nextVal();
        SKAPI bool hasNext();
        iterator(Iterator * pIter);
    private:
        Iterator * pIterImpl;
    };

  SKAPI bool empty() const;
  SKAPI unsigned int size () const;
  SKAPI typename JSKMap::iterator * getIterator();
  SKAPI ~JSKMap ();
  JSKMap (Set * pSet); //will own/delete the pointer
  JSKMap (Map * pMap); //will own/delete the pointer
private:
  Set * pEntrySet;
};
typedef JSKMap<SKVal*>          JStrValMap;
typedef JSKMap<SKStoredValue*> JStrSVMap;

typedef SKVector<std::string>               StrVector;
typedef SKVector<const std::string*>      KeyVector;
typedef SKMap<std::string, std::string>      StrStrMap;
typedef SKMap<std::string, SKVal*>          StrValMap;
typedef SKMap<std::string, SKStoredValue*> StrSVMap;
typedef SKMap<string,SKOperationState::SKOperationState>   OpStateMap;
typedef SKMap<string,SKFailureCause::SKFailureCause>     FailureCauseMap;


#endif   //SKCONTAINERS_H
