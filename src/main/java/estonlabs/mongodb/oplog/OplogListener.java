package estonlabs.mongodb.oplog;

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
 * Listens to events from the oplog
 * 
 * @author antlen
 *
 */
public interface OplogListener {	
	
	/**
	 * The document that was inserted into the namespace
	 * @param doc
	 */
	void onInsert(Document doc);
	
	/**
	 * The new version of the document that was updated
	 * 
	 * @param doc
	 */
	void onUpdate(Document doc);
	
	/**
	 * The id of the document that was deleted.
	 * 
	 * @param id
	 */
	void onDelete(String id);
}
