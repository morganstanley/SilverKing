// This is the main DLL file.

#include "stdafx.h"

#include <string>
#include <map>
#include <stdlib.h> 

#include "SKManagedClient.h"
#include "MClientDHTConfiguration.h"
#include "MSession.h"
#include "MSessionOptions.h"
#include "MClientDHTConfigurationProvider.h"
#include "MValueCreator.h"

#include "skbasictypes.h"
#include "skcontainers.h"
#include "SKClient.h"
#include "SKGridConfiguration.h"
#include "SKSession.h"
#include "SKSessionOptions.h"
#include "SKClientDHTConfiguration.h"
#include "SKClientDHTConfigurationProvider.h"
#include "MValueCreator.h"


#include "SKAsyncNSPerspective.h"
#include "SKSyncNSPerspective.h"
#include "SKStoredValue.h"
#include "SKValueCreator.h"
#include "SKClientDHTConfiguration.h"
#include "SKSessionOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"
#include "SKNamespaceOptions.h"
#include "SKAsyncPut.h"
#include "SKAsyncRetrieval.h"
#include "SKAsyncValueRetrieval.h"
#include "SKAsyncSingleValueRetrieval.h"
#include "SKAsyncSyncRequest.h"
#include "SKAsyncSnapshot.h"

using namespace std;

using namespace System::Net;
using namespace System::IO;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;

namespace SKManagedClient {

String ^ MValMetaData::ToString(){
		  IPAddress ^ ipAddr = gcnew IPAddress(BitConverter::GetBytes(creatorIP));
          String ^ str = gcnew String("ABC");
          str = "uncompressLength: " + uncompressLength +
                "\nstoredLength:    " + storedLength +
                "\ncompression:     " + compression.ToString() +
                "\nchecksumType:    " + checksumType.ToString() +
                "\nversion:         " + version +
                "\ncreationTime:    " + creationTime.ToString() +
				"\ncreatorIP:       " + ipAddr->ToString() +
				"\ncreatorID:       " + creatorID;
          if (userData != nullptr)
          {
              str += 
				"\nuserData:        " + userData;
          }

          return str;
}

}

