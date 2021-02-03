/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef DHTCOMMON_H
#define DHTCOMMON_H

/////////////
// includes
#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;

#include "jace/StaticVmLoader.h"
using jace::StaticVmLoader;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/JClass.h"
using jace::JClass;
#include "jace/JClassImpl.h"
using jace::JClassImpl;
#include "jace/JNIException.h"
using jace::JNIException;
#include "jace/OptionList.h"
using jace::OptionList;
#include "jace/VirtualMachineShutdownError.h"
using jace::VirtualMachineShutdownError;
#include "jace/proxy/types/JInt.h"
using jace::proxy::types::JInt;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;
#include "jace/proxy/types/JLong.h"
using jace::proxy::types::JLong;
#include "jace/proxy/JObject.h"
using jace::proxy::JObject;
#include "jace/proxy/types/JBoolean.h"
using jace::proxy::types::JBoolean;


#include "jace/proxy/java/lang/Class.h"
using jace::proxy::java::lang::Class;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/System.h"
using jace::proxy::java::lang::System;
#include "jace/proxy/java/lang/Object.h"
using ::jace::proxy::java::lang::Object;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jace/proxy/java/io/PrintWriter.h"
#include "jace/proxy/java/io/IOException.h"
using ::jace::proxy::java::io::IOException;
#include "jace/proxy/java/io/PrintStream.h"
using ::jace::proxy::java::io::PrintStream;
using namespace jace::proxy::java::lang;
using namespace jace::proxy::java::io;

#include "jace/proxy/java/nio/ByteBuffer.h"
using ::jace::proxy::java::nio::ByteBuffer;


#include "jace/proxy/java/util/Set.h"
using jace::proxy::java::util::Set;
#include "jace/proxy/java/util/List.h"
using jace::proxy::java::util::List;
#include "jace/proxy/java/util/Map.h"
using jace::proxy::java::util::Map;
#include "jace/proxy/java/util/HashSet.h"
using jace::proxy::java::util::HashSet;
#include "jace/proxy/java/util/HashMap.h"
using jace::proxy::java::util::HashMap;
#include "jace/proxy/java/util/Map_Entry.h"
using jace::proxy::java::util::Map_Entry;
#include "jace/proxy/java/util/Iterator.h"
using jace::proxy::java::util::Iterator;
#include "jace/proxy/java/util/concurrent/TimeUnit.h"
using jace::proxy::java::util::concurrent::TimeUnit;
#include "jace/proxy/java/util/logging/Level.h"
using jace::proxy::java::util::logging::Level;

#include "jace/proxy/com/google/common/base/Throwables.h"
using jace::proxy::com::google::common::base::Throwables;

#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;
#include "jace/proxy/com/ms/silverking/cloud/dht/ConsistencyProtocol.h"
using jace::proxy::com::ms::silverking::cloud::dht::ConsistencyProtocol;
#include "jace/proxy/com/ms/silverking/cloud/dht/CreationTime.h"
using jace::proxy::com::ms::silverking::cloud::dht::CreationTime;
#include "jace/proxy/com/ms/silverking/cloud/dht/GetOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::GetOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceCreationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceCreationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceCreationOptions_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceCreationOptions_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceVersionMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceVersionMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/NodeID.h"
using jace::proxy::com::ms::silverking::cloud::dht::NodeID;
#include "jace/proxy/com/ms/silverking/cloud/dht/NonExistenceResponse.h"
using jace::proxy::com::ms::silverking::cloud::dht::NonExistenceResponse;
#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalType.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalType;
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/RevisionMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::RevisionMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/SessionOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::SessionOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/StorageType.h"
using jace::proxy::com::ms::silverking::cloud::dht::StorageType;
#include "jace/proxy/com/ms/silverking/cloud/dht/TimeoutResponse.h"
using jace::proxy::com::ms::silverking::cloud::dht::TimeoutResponse;
#include "jace/proxy/com/ms/silverking/cloud/dht/ValueCreator.h"
using jace::proxy::com::ms::silverking::cloud::dht::ValueCreator;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;

#include "jace/proxy/com/ms/silverking/cloud/dht/common/NamespaceOptionsClient.h"
using jace::proxy::com::ms::silverking::cloud::dht::common::NamespaceOptionsClient;
#include "jace/proxy/com/ms/silverking/cloud/dht/common/DHTUtil.h"
using jace::proxy::com::ms::silverking::cloud::dht::common::DHTUtil;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/AbsMillisVersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AbsMillisVersionProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsynchronousReadableNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsynchronousReadableNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsynchronousWritableNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsynchronousWritableNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncKeyedOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncKeyedOperation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncPut.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncPut;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncValueRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncValueRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSingleRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSingleRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSingleValueRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSingleValueRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSyncRequest.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSyncRequest;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSnapshot.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSnapshot;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/BaseNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::BaseNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ChecksumType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ChecksumType;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfiguration.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfiguration;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfigurationProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfigurationProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/Compression.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::Compression;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/DHTClient.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::DHTClient;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/DHTSession.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::DHTSession;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/FailureCause.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::FailureCause;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/KeyDigestType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::KeyDigestType;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/KeyedOperationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::KeyedOperationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OperationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OperationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OperationState.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OperationState;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/MetaData.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::MetaData;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/Namespace.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::Namespace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceCreationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceCreationException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceLinkException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceLinkException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/PutException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::PutException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/RelNanosVersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::RelNanosVersionProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/RetrievalException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::RetrievalException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SnapshotException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SnapshotException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SyncRequestException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SyncRequestException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/WaitForCompletionException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::WaitForCompletionException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValue.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValueBase.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValueBase;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SynchronousReadableNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SynchronousReadableNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SynchronousWritableNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SynchronousWritableNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/VersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::VersionProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SimpleTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SimpleTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpSizeBasedTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpSizeBasedTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/WaitForTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::WaitForTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SecondaryTargetType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SecondaryTargetType;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceDeletionException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceDeletionException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceRecoverException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceRecoverException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SessionEstablishmentTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SessionEstablishmentTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SimpleSessionEstablishmentTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SimpleSessionEstablishmentTimeoutController;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/DHTSessionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::DHTSessionImpl;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/AsynchronousNamespacePerspectiveImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::AsynchronousNamespacePerspectiveImpl;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/SynchronousNamespacePerspectiveImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::SynchronousNamespacePerspectiveImpl;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/PutExceptionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::PutExceptionImpl;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/RetrievalExceptionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::RetrievalExceptionImpl;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/SyncRequestExceptionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::SyncRequestExceptionImpl;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/BufferDestSerializer.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::BufferDestSerializer;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/BufferSerDes.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::BufferSerDes;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/BufferSourceDeserializer.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::BufferSourceDeserializer;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/ByteArraySerDes.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::ByteArraySerDes;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/ObjectSerDes.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::ObjectSerDes;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/RawByteArraySerDes.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::RawByteArraySerDes;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/SerializationRegistry.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::SerializationRegistry;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/serialization/StringSerDes.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::serialization::StringSerDes;

#include "jace/proxy/com/ms/silverking/cloud/dht/gridconfig/SKGridConfiguration.h"
using jace::proxy::com::ms::silverking::cloud::dht::gridconfig::SKGridConfiguration;

#include "jace/proxy/com/ms/silverking/cloud/dht/ForwardingMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::ForwardingMode;

#include "jace/proxy/com/ms/silverking/net/HostAndPort.h"
using jace::proxy::com::ms::silverking::net::HostAndPort;
#include "jace/proxy/com/ms/silverking/net/AddrAndPort.h"
using jace::proxy::com::ms::silverking::net::AddrAndPort;
#include "jace/proxy/com/ms/silverking/net/IPAndPort.h"
using jace::proxy::com::ms::silverking::net::IPAndPort;

#include "jace/proxy/com/ms/silverking/thread/lwt/LWTPoolProvider.h"
using jace::proxy::com::ms::silverking::thread::lwt::LWTPoolProvider;
#include "jace/proxy/com/ms/silverking/thread/lwt/DefaultWorkPoolParameters.h"
using jace::proxy::com::ms::silverking::thread::lwt::DefaultWorkPoolParameters;

#include "jace/proxy/com/ms/silverking/time/AbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::AbsMillisTimeSource;
#include "jace/proxy/com/ms/silverking/time/RelNanosAbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosAbsMillisTimeSource;
#include "jace/proxy/com/ms/silverking/time/RelNanosTimeSource.h"
using jace::proxy::com::ms::silverking::time::RelNanosTimeSource;
#include "jace/proxy/com/ms/silverking/time/Stopwatch.h"
using jace::proxy::com::ms::silverking::time::Stopwatch;
#include "jace/proxy/com/ms/silverking/time/ConstantAbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::ConstantAbsMillisTimeSource;
#include "jace/proxy/com/ms/silverking/time/SimpleNamedStopwatch.h"
using jace::proxy::com::ms::silverking::time::SimpleNamedStopwatch;
#include "jace/proxy/com/ms/silverking/time/SimpleStopwatch.h"
using jace::proxy::com::ms::silverking::time::SimpleStopwatch;
#include "jace/proxy/com/ms/silverking/time/StopwatchBase.h"
using jace::proxy::com::ms::silverking::time::StopwatchBase;
#include "jace/proxy/com/ms/silverking/time/SystemTimeSource.h"
using jace::proxy::com::ms::silverking::time::SystemTimeSource;
#include "jace/proxy/com/ms/silverking/time/TimerDrivenTimeSource.h"
using jace::proxy::com::ms::silverking::time::TimerDrivenTimeSource;
#include "jace/proxy/com/ms/silverking/time/Stopwatch_State.h"
using jace::proxy::com::ms::silverking::time::Stopwatch_State;

//using namespace  jace::proxy::com::ms::silverking::cloud::dht;
//using namespace  jace::proxy::com::ms::silverking::cloud::dht::client;
//using namespace  jace::proxy::com::ms::silverking::cloud::dht::client::impl;

typedef JArray< jace::proxy::types::JByte > ByteArray;
 
#endif   //DHTCOMMON_H
