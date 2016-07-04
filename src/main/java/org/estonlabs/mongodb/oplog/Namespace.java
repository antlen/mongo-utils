package org.estonlabs.mongodb.oplog;
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
 * A namespace in the mongo db.  
 * 
 * @author antlen
 *
 */
public final class Namespace {
	private final String database;
	private final String colleciton;
	private final String key;

	/**
	 * Neither the database nor the collection can be null.
	 * 
	 * @param database
	 * @param colleciton
	 */
	public Namespace(String database, String colleciton) {
		if(database == null) throw new IllegalArgumentException("The database cannot be null.");
		if(colleciton == null) throw new IllegalArgumentException("The colleciton cannot be null.");
		this.database = database;
		this.colleciton = colleciton;
		this.key = database + "." + colleciton;
	}


	public String getDatabase() {
		return database;
	}

	public String getColleciton() {
		return colleciton;
	}
	
	public String getKey(){
		return key;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + colleciton.hashCode();
		result = prime * result + database.hashCode();
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Namespace other = (Namespace) obj;
		if (!colleciton.equals(other.colleciton))
			return false;
		if (!database.equals(other.database))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "Namespace [database=" + database + ", colleciton=" + colleciton + "]";
	}
	
	
}
