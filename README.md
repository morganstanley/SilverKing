# SilverKing
**Scalable, high-throughput storage and coordination**

## Introduction

SilverKing is a simple, scalable, high-throughput data **storage** and 
**coordination** mechanism designed for use in distributed applications. 
SilverKing provides both a distributed key-value store and a distributed file system.


### What makes SilverKing compelling?

SilverKing has several unique features that make it an attractive solution for many use-cases.

* **Extreme scale** - SilverKing supports many tens of thousands of simultaneous clients, 
  at least thousands of storage servers, and storage limited by the aggregate capacity of the server disks. 
* **Rich topology support** - Users can directly specify high-level storage policies such as: 
"primary replicas in New York and London. Secondary replicas in all other regions. 
Within each regional data center, each primary value must be stored in two racks, and every rack must have a 
secondary replica." We are not aware of any existing solution that provides similar support in an automated fashion.
* **Speed** - SilverKing is as fast as the best RAM-only distributed key-value store (that we are aware of), 
but also supports disk storage.
* **Memory efficiency** - SilverKing is more memory efficient than existing RAM-only distributed key-value stores.
* **Coordination primitives** - In addition to pure storage, SilverKing supports Linda-like coordination primitives. 
This makes writing distributed applications far easier than with conventional techniques.
* **Scale out existing vendor file systems** - SKFS enables snapshot-capable file systems  
to scale out far more powerfully and more economically than with vendor solutions alone. In production this enables 
volume snapshots to be used on large computational grids. (This could be used, for example, to scale out an 
existing vendor volume for use on a Hadoop cluster.)
* SilverKing's file system - SKFS - inherits the key-value store's scale, speed, and topology support.

## Supported Storage Interfaces

### Key-value Store

SilverKing provides a hash table-like interface that is accessible in a distributed environment. 
Values may be stored and retrieved from any server. 
Linda-like coordination primitives simplify distributed application development. 

### File System

The SilverKing File System (SKFS) provides a highly-scalable file system by leveraging the SilverKing DHT. 
SKFS is the successor to the SRFS project.


## Topology Support

SilverKing provides rich inter and intra-datacenter topology support as well as support
for both primary and secondary replicas (writes are always reflected in primary replicas,
and will eventually be reflected in secondary replicas.)


## Scale and Performance

SilverKing is designed to support extremely demanding distributed storage and coordination.
Many tens of thousands of clients may simultaneously utilize a common SilverKing instance.
This allows SilverKing to support the most demanding "Big Data" applications in addition
to less-demanding distributed applications.

Specifically, SilverKing provides scale and performance along many dimensions such as:

* **Operation throughput**: many tens of millions of operations per second for large instances
* **Data throughput**: limited by the network for large data items
* **Latency**: < 200 us average for sustained operations contained within a rack
* **Clients**: at least tens of thousands of concurrent writers (and readers) for large instances
* **Storage**: limited by the aggregate capacity of all hard drives in use.



## Topology Changes and Failure Handling

SilverKing supports live topology and configuration changes. For instance, it is possible to add or
remove servers while SilverKing is running. It is also possible to change the replication level, the
fraction of data stored on each server, the topology structure, etc. all without the need to restart
SilverKing.


The same mechanisms that make it possible to support live topology changes, also enable 
SilverKing to function in the presence of failures (within the realm of what is 
feasible given the storage policy and level of failure.)


## Client APIs

APIs are available for the following languages: Java, C++, Perl, and q. 
See the Client Primer for a brief introduction to writing client applications.
Javadoc documentation is available for the Java API.

## Licensing

See LICENSE.md for SilverKing licensing terms. See lib/LICENSE.README.md for licensing terms of libraries used by SilverKing.
