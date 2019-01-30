[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](http://www.scala-lang.org/api/2.11.x/)
[![Scala version](https://img.shields.io/badge/scala-2.12-orange.svg)](http://www.scala-lang.org/api/2.12.x/)
[![Scala version](https://img.shields.io/badge/scala-2.13-orange.svg)](http://www.scala-lang.org/api/2.13.x/)


[ ![Download](https://api.bintray.com/packages/bulletproof-ninja/maven/Delta/images/download.svg) ](https://bintray.com/bulletproof-ninja/maven/Delta/_latestVersion)
[![Join Chat at https://gitter.im/bulletproof-ninja/delta](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/bulletproof-ninja/delta)


# Delta [![Build Status](https://semaphoreci.com/api/v1/bulletproof-ninja/delta/branches/master/badge.svg)](https://semaphoreci.com/bulletproof-ninja/delta)


![Delta](https://i.imgur.com/gjBAIKw.png)


DDD-centric event-sourcing library for the JVM.

---------------------

## What's in a name?

### del·ta
#### /ˈdeltə/
_symbol_
symbol: δ; symbol: Δ; symbol: delta
1.
MATHEMATICS
variation of a variable or function.
2.
MATHEMATICS
a finite increment.

\- or -

### [Delta encoding](https://en.wikipedia.org/wiki/Delta_encoding)
> Delta encoding is a way of storing or transmitting data in the form of differences (deltas) between sequential data

----------------------

## Design principles

- Non-blocking code, `Future` based API.
- Pluggable architecture. Storage and messaging are implementation neutral. With multiple supported implementations, it's easier to use with existing infrastructure. Current implementations:
  - Storage:
    - Generic JDBC, with MySQL, and PostgreSQL adaptations. Custom adaptions are straightforward.
    - MongoDB
    - Cassandra
  - Messaging:
    - Redis Pub/Sub
    - Hazelcast Topic
- No third-party dependencies (except for storage/messaging implementations)
- Minimally invasive. Large degree of freedom in implementation. Uses type classes instead of inheritance. No annotation hell.
- Fast, scalable. Written for very large data sets; supports both horizontal and vertical scaling.
- Customizable. Attempts to provide sane defaults, but enables high degree of customization.

----------------------

## Why event-sourcing?

- Complete data history. Intrinsically complete and correct audit trail of any and all changes.
- Very fast. Append-only writes. Prebuilt reads (push and pull).
- Scalable; effortless sharding.
- No object-relational mismatch.
- No RDBMS schema upgrades.
- Fully separate OLTP/OLAP.
- Easy and flexible authentication. Any and all updates are done through explicit, use case specific, commands, each of which can have custom authentication, if required.
