package estonlabs.mongodb.oplog;

import java.util.*;

import org.bson.Document;

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
 * Models the three type of events present in the oplog
 * 
 * @author antlen
 *
 */
public enum OplogEventType {

	INSERT("i", "o"){
		@Override
		public void notify(OplogListener listener, Document doc) {
			listener.onInsert(doc);
		}
	}, 
	UPDATE("u", "o2"){
		@Override
		public void notify(OplogListener listener, Document doc) {
			listener.onUpdate(doc);
		}
	}, 
	DELETE("d", "o"){
		@Override
		public void notify(OplogListener listener, Document doc) {
			listener.onDelete(doc.getString("_id"));
		}
	};
	
	private static final Map<String, OplogEventType> map;
	
	static{
		Map<String, OplogEventType> m = new HashMap<>();
		for(OplogEventType t : values()){
			m.put(t.operation, t);
		}
		map = Collections.<String, OplogEventType>unmodifiableMap(m);
	}
	
	private final String operation;
	private final String objectPropertyId;

	private OplogEventType(final String operation, String objectPropertyId) {
		this.operation = operation;
		this.objectPropertyId = objectPropertyId;
	}
	
	public static OplogEventType getOplogEventType(String s){
		return map.get(s);
	}

	public String getObjectPropertyId() {
		return objectPropertyId;
	}
	
	public abstract void notify(OplogListener listener, Document doc);
}
