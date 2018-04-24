#include "jace/JMethod.h"

#include "jace/JArguments.h"
using jace::JArguments;

#include "jace/proxy/JValue.h"
using jace::proxy::JValue;

#include <list>
using std::list;

#include <vector>
using std::vector;

BEGIN_NAMESPACE(jace)


/**
 * Transforms a JArguments to a vector of jvalue's.
 */
vector<jvalue> toVector(const JArguments& arguments)
{
  typedef list<const JValue*> ValueList;
  vector<jvalue> argsVector;
  ValueList argsList = arguments.asList();

  ValueList::iterator end = argsList.end();
  for (ValueList::iterator i = argsList.begin(); i != end; ++i)
    argsVector.push_back(static_cast<jvalue>(**i));

  return argsVector;
}

END_NAMESPACE(jace)

/**
 * For those (oddball) compilers that need the template specialization
 * definitions in the header.
 */
#ifndef PUT_TSDS_IN_HEADER
  #include "jace/JMethod.tsd"
#endif
