package org.estonlabs.mongodb.oplog;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.*;
import org.slf4j.*;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;

/**
 * The MIT License (MIT)
 *	
 *	Copyright (c) 2016 antlen
 *	
 *	Permission is hereby granted, free of charge, to any person obtaining a copy
 *	of this software and associated documentation files (the "Software"), to deal
 *	in the Software without restriction, including without limitation the rights
 *	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *	copies of the Software, and to permit persons to whom the Software is
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in all
 *	copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *	SOFTWARE.
 * 
 * 
 * 
 * 
 *   Monitors the oplog of the mongo client and will call back on the listens when events are detected.
 * 
 *  <pre>
 *  {@code
 *		final MongoClient mongoClient = new MongoClient("localhost" , 3001 );
 *		
 *      final OplogMonitor mongo = new OplogMonitor(mongoClient);
 *		
 *		mongo.start();
 *
 * 		final Namespace namespace = new Namespace( "meteor", "markets" );		
 *
 *		//only listen to the insert and delete events.  
 *		mongo.listenToNameSpace(namespace, this, OplogEventType.INSERT, OplogEventType.DELETE);
 * 
 * 
 * 	    //when finished monitoring call
 *      mongo.stop();	
 *  }
 *  </pre>
 * @author antlen
 *
 */
public class OplogMonitor {
	private static final Logger LOGGER = LoggerFactory.getLogger(OplogMonitor.class);
	
	private static final String OPLOG = "oplog.rs";
	private static final String LOCAL_DB = "local";
	
	private final MongoClient mongoClient;
	private final ListenerCollection listeners = new ListenerCollection();
	
	private OplogTail tail = new OplogTail();
	
	public OplogMonitor(MongoClient cl){
		mongoClient = cl;
	}
	

	/**
	 * Starts listening to the oplog on a separate thread.  
	 *
	 * @throws IllegalStateException - if the monitor is already running.
	 */
	public synchronized void start() throws IllegalStateException{
		if(isRunning()) throw new IllegalStateException("OplogMonitor is already running.");
	  
		tail.start();
	}
	
	/**
	 * Stops monitoring.  
	 */
	public synchronized void stop(){
		tail.stop();
	}
	
	public boolean isRunning(){
		return tail.isActive.get();
	}
	
	/**
	 * Listens to all events for the mongo namespace
	 * 
	 * @param ns
	 * @param listener
	 */
	public void listenToNameSpace(Namespace ns, OplogListener listener){
		listenToNameSpace(ns, listener, OplogEventType.values());
	}
	
	/**
	 * Listens to events of the specified type for the mongo namespace
	 * 
	 * 
	 * @param ns
	 * @param listener
	 * @param types
	 */
	public void listenToNameSpace(Namespace ns, OplogListener listener, OplogEventType ... types){		
		listeners.addListener(listener, ns.getKey(), types);
	}
	
	/**
	 * Removes the listener if it was present.
	 * 
	 * @param l
	 * @return
	 */
	public boolean removeListener(OplogListener l){
		return listeners.remove(l);
	}
	
	private class OplogTail implements Runnable{

		protected final AtomicBoolean isActive = new AtomicBoolean(false);
		private Thread runner;
		private MongoCursor<Document> cursor;
		
		public void run() {		
		    while(cursor.hasNext() && isActive.get()) { // will block until the next record is added into the oplog
		    	try{
					final Document o = cursor.next();
				
					OplogEventType updateType = OplogEventType.getOplogEventType(o.getString("op"));
					for(OplogListener ul : listeners.get(o.getString("ns"), updateType)){
						final Document doc =  (((Document)o.get(updateType.getObjectPropertyId()))); 
						updateType.notify(ul, doc);
					}
					
		    	}
		    	catch(Exception e){
		    		LOGGER.error("Error while polling the oplog", e);
		    	}
			}
		}

		protected void start(){
			isActive.set(true);
			final MongoCollection<Document> oplog = getOpLog();
			cursor = createTail(oplog);
			//call run and execute in a new thread
			runner = new Thread(this);
			runner.start();
		}
		
		protected void stop(){
			isActive.set(false);
			if(runner != null)runner.interrupt();
			runner = null;
			cursor = null;
		}
		
		/**
		 * sorts by reverse chronological order and restricts to one record.
		 * @param records
		 * @return
		 */
		protected FindIterable<Document> findLastRecord(final FindIterable<Document> records)
		{
			return records.sort(new BasicDBObject("ts",-1)).limit(1);
		}
		
		/**
		 * Returns the timestamp of the first record in the collection.
		 * 
		 * @param records
		 * @return
		 */
		protected BsonTimestamp getFirstTimestamp(final FindIterable<Document> records)
		{
			Document first = records.first();
			return first==null? null : (BsonTimestamp)first.get("ts");
		}
		
		private MongoCursor<Document> createTail(final MongoCollection<Document> oplog) 
		{
		    //get the timestamp of the last record in the oplog.
		    final BsonTimestamp lastEntryTime = getFirstTimestamp(findLastRecord(oplog.find()));
			
		    //find all records after the last timestamp.
			final FindIterable<Document>  resultset = find(oplog, lastEntryTime).noCursorTimeout(true);
			resultset.cursorType(CursorType.Tailable); // make sure the iterator is tailable.
			
			return  resultset.iterator();
		}

		private FindIterable<Document> find(final MongoCollection<Document> oplog, final BsonTimestamp lastEntryTime) {
			if(lastEntryTime == null){
				//this is rare.  It means there were no entries in the oplog
				LOGGER.info("Tailing all events");
				return oplog.find();
			}else{
				LOGGER.info("Tailing for events after " + lastEntryTime.toString());
				return oplog.find(Filters.gt("ts", lastEntryTime));
			}
		}
		
		private MongoCollection<Document> getOpLog() {
			final MongoDatabase local = mongoClient.getDatabase(LOCAL_DB);
		    return local.getCollection(OPLOG);
		}	
	}
	
	private class ListenerCollection {
		private final Iterable<OplogListener> empty = Collections.<OplogListener> emptyList();
		private final ConcurrentHashMap<String, Map<OplogEventType,Set<OplogListener>>>listeners = new ConcurrentHashMap<>();
		
		public void addListener(OplogListener listener, String ns, OplogEventType... types) {
			final Map<OplogEventType, Set<OplogListener>> m = getListenerMap(ns);
			for(OplogEventType t : types){
				Collection<OplogListener> l = m.get(t);
				l.add(listener);	
			}
		}
		
		public boolean remove(OplogListener l){
			boolean removed = false;
			for(Map<OplogEventType,Set<OplogListener>> m : listeners.values()){
				for(Set<OplogListener> s : m.values()){
					removed |= s.remove(l);
				}
			}
			return removed;
		}
		
		public Iterable<OplogListener> get(String ns, OplogEventType t){
			final Map<OplogEventType,Set<OplogListener>> m = listeners.get(ns);
			
			return m == null ? empty : m.get(t);
		}
		
		private Map<OplogEventType, Set<OplogListener>> getListenerMap(String ns) {
			final Map<OplogEventType,Set<OplogListener>> m = listeners.get(ns);
			if(m == null){
				Map<OplogEventType,Set<OplogListener>> newMap = buildEnumMap();
				return listeners.putIfAbsent(ns, newMap) == null ? newMap : listeners.get(ns);
			}
			return m;
		}
		
		private Map<OplogEventType, Set<OplogListener>> buildEnumMap() {
			final Map<OplogEventType, Set<OplogListener>> m = new EnumMap<>(OplogEventType.class);
			for(OplogEventType t : OplogEventType.values()){
				m.put(t, new CopyOnWriteArraySet<>());
			}
			return m;
		}
	}
}
