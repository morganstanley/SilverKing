/****
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKBASENSPERSPECTIVE_H
#define SKBASENSPERSPECTIVE_H

#include <stdint.h>
#include <string>
using std::string;
#include "skconstants.h"
#include "skbasictypes.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class BaseNamespacePerspective;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::BaseNamespacePerspective BaseNamespacePerspective;
class SKNamespace;
class SKNamespacePerspectiveOptions;
class SKVersionConstraint;
class SKVersionProvider;

class SKBaseNSPerspective
{
public:
  SKAPI virtual ~SKBaseNSPerspective();
  SKAPI virtual std::string getName();
  SKAPI virtual SKNamespace * getNamespace();
  SKAPI virtual SKNamespacePerspectiveOptions * getOptions();
  SKAPI virtual void setOptions(SKNamespacePerspectiveOptions * nspOptions);
  SKAPI virtual void setDefaultRetrievalVersionConstraint(SKVersionConstraint * vc);
  SKAPI virtual void setDefaultVersionProvider(SKVersionProvider * versionProvider);
  SKAPI virtual void setDefaultVersion(int64_t version);
  SKAPI virtual void close();

protected:
  virtual void * getPImpl()=0;
};


#endif  //SKBASENSPERSPECTIVE_H
