## Opinion on Those Questions

First section answers questions mentioned in task1. I will describe what issues I found
in this source code.

#### Does the library fulfill the requirements described in the background section?

At first glance, it seems to satisfy the requirement described in background section,
but here is some **potential bugs** and **data-racing problem** and **performance issue** in this code.
Thus It may not run as our expectation. For example, for query function defined in this
r-w splitting db structure, it naively picks up a replica and query through it. However,
the problem is, querying through it may get error because the replica is in periodical
maintenance, but actually we are allowed to choose another replica to try our query because
of the at-least-one-replica-up condition.

It can be better if we do or solve exceptional handling, thread-safe problem, error handling,
and concurrently querying replicas using go-routine.
Besides, it may not run as our expectation. For example, it does not query by one of replica
as our expectation when someone just wants a read operation (ie. `SELECT * FROM table`) by
`Exec()` method. In this case, it may need to analyse the sql statement to correct it.

#### Is the library easy to use?

In my opinion, it has clear interface to make programmer easily use it, but perhaps
we can abstract those method set as an interface so that it gets more modular. That is,
if there is another sql read-write strategy, all our developer has to do is to implement
another structure satistied the interface. Hence just need to return another implementation of
this interface rather than modify the usage or type of specified structure in code base.

Besides, there is definition problem about function `Ping()` and `PingContext()`, because
in this initialized source code, it returns error once either any of replica gets error or
master gets error. However, actually the db object still usable only if at least one replica
still gets connected. Thus the behavior of both of these two function should be defined
and revised.

#### Is the code quality assured?

Maybe we can write some unittest to make sure the output of each method is always as
our expectation, and integrate the unittest to CI/CD flow to ensure the quality of
pre-prodction code.

#### Is the code readable?

It's a clear and simple code. The naming of variables and functions make sense, and
there is no redundant code, but still has potential bugs so that it may be out of our
expectation. I think it's readable, at least for me.

#### Is the library thread-safe?

Definitely not. The count variable should be atomically updated. Otherwise it causes
data racing if we naively do nothing but just run `db.count++`. In this case, we can
replace it with either lock mechanism or `AddInt()` in built-in library atomic to prevent
it from happening.

## Idea and Implementation

So, next step is, how can I improve it? Following describe what I did and how could
I make it better by fixing thoses potential problem mentioned above.

#### Query for replica
- RandomStartRoundRobin

In the original source code, the data racing issue occurs in the step of picking up a
replica without any lock mechanism. Here I replaced this strategy with another method
to prevent it from data racing but still no need of any lock. The idea is, for each
query, pick up a random numebr in the range `[0, numReplica)` and treat it as our starting
index to try our query, if the query for replica of initialized index gets failure, the
index moves on to next one just like roundrobin method until it runs over all replicas.
By doing so, we do not need the member variable `db.count` anymore so that there is no
data racing issue. Note that in the details of implementation, before query for the
specific replica, it first check if this replica is still connected or not by the **cache state**
so as to avoid redundant tries for any disconnected replica. Besides, if any query for a
connected replica gets failure, it sends a signal via go-channel to notify **state checker**
to ping the replica and change state of connection if it cannot be reached.

Hence, the overall logic is:
```
number = random choose any index in [0, numReplica)
for index from number to number+numReplica:
	actualIndex = index%numReplica
	if replica indexed by actuallyIndex is alive:
		err = query for it
		if err:
			notify checker to check replica of actualIndex
```

- Defer to QueryRow

Because the error value in returned structure of built-in `Rows` is not exposed to caller,
thus it's not allowed to fetch the error from this object. Therefore another auxiliary
struct was introduced to make it still be fluent in the same way of built-in method. That is,
execution of `QueryRow()` is defered until invoke `Scan()` method. `QueryRow().Scan()`
fluent interface remains in this package.

#### State checker

A state checker is a go-routine launched when initializing a DB instance. It's geared to
periodically check the state of master and replicas. It's a infinit for loop listening
4 channels:

1. A timer channel ringing every 30 seconds. Once timer rings, concurrently check state
of master and all replicas by invoking `CheckConnection()`.
2. A channel receiving notification of checking state of master.
3. A channel receiving notification of checking state of replica given index.
4. Shutdown channel, which aims to gracefully shutdown.

The state checking channels receive notification only when either any read operation (`Query()`)
gets failed or any write operation (`Exec(), Begin(), Prepare()`) gets error. Once it receives
notification, it immediately checks state of the corresponding DB and updates if state changes.

#### Cached state of connection

`insatance` is a wrapper structure to `sql.DB`, which is reponsible for caching state of db and
possibly further checking connection if someone calls `CheckConnection()`. The method `CheckConnection`
utilizes built-in atomic operation to implement a lock-free mechanism to get/set state of DB.
The behavior of `CheckConnection()` is: First check the last check time, if time difference between
now time and last check time is less than 5 seconds, just do nothing but return cached state. If not,
try to get lock by using `atomic.CompareAndSwap()` to prevent it from **cache avalanche**.
The one successfully get the lock is allowed to ping the target DB instance and update cached state atomically.
The logic described above can be written in pesudo code:

```
if nowTime - lastCheckTime < 5 seconds:
	return nowState
if fail to getLock:
	return nowState
newState = ping DB
if nowState != newState:
	atomically update the cached state
return newState
```

By utilizing atomic library, `CheckConnection()` was implemented in a lock-free way, so it's no doubt a
thread-safe function.

#### Unittest

The file mydb_test.go imports third party library go-sqlmock and testify/suite to test those methods.
Even if it's in a scenario that some replicas get disconnected, it still works well as expectation.

## Conlusion

By solving potential issues above and improving using concurrent go-routines, this package is ready
to be used in production environment.
