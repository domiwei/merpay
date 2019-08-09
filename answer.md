* Does the library fulfill the requirements described in the background section?
   At first glance, it almost satisfies the requirement described in background section,
   but here is some potential bugs and data-racing problem and performance issue in this code.
   It can be better if we do or solve exceptional handling, thread-safe problem, error handling,
   and concurrently checking replicas using go-routine.
   Besides, it may not run as our expectation. For example, it does not query by one of replica
   as our expectation when someone just wants a read operation (ie. SELECT * FROM table) by
   Exec() method. In this case, it may need to analyse the sql statement to correct it.

* Is the library easy to use?
   In my opinion, it has clear interface to make programmer easily use it, but perhaps
   we can abstract those method set as an interface so that it gets more modular. That is,
   if there is another sql read-write strategy, all developer has to do is to implement another
   structure satistied the interface. Hence just need to return another implementation of
   this interface rather than modify the usage or type of specified structure in code base.

* Is the code quality assured?
   Maybe we can write some unittest to make sure the output of each method is always as
   our expectation, and integrate the unittest to CI/CD flow to ensure the quality of
   pre-prodction code.

* Is the code readable?
   It's a clear and simple code. The naming of variables and functions make sense, and
   there is no redundant code or dead logic. I think it's readable, at least for me.

* Is the library thread-safe?
   Definitely not. The count variable should be atomically updated. Otherwise it causes
   data racing if we naively do nothing but just run db.count++. In this case, we can
   replace it with AddInt in built-in library atomic to prevent it from happening.
