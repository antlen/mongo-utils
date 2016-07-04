# mongo-utils

A utility for subscribing to a mongo collction for updates whenever there is an insert, update or delete.


```xml
	<dependency>
	   <groupId>org.estonlabs</groupId>
	  <artifactId>mongo-utils</artifactId>
	  <version>1.0.0</version>
    </dependency>
```



```java
	 final MongoClient mongoClient = new MongoClient("localhost" , 3001 );	
     final OplogMonitor mongo = new OplogMonitor(mongoClient);
     mongo.start();
	
	 final Namespace namespace = new Namespace( "meteor", "markets" );		

	//only listen to the insert and delete events.  
	mongo.listenToNameSpace(namespace, this, OplogEventType.INSERT, OplogEventType.DELETE);
 
 
 	  //sometime later when finished monitoring call
      mongo.stop();
```