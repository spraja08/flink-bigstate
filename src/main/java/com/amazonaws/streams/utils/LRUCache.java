package com.amazonaws.streams.utils;

import java.util.*;

public class LRUCache<K,V> extends LinkedHashMap<K,V> {
	private static final long serialVersionUID = 1L;
	private int capacity;

	public LRUCache( int capacity, float d ){
		super( capacity, d, true );
		this.capacity = capacity;
	}

	/**
	 * removeEldestEntry() should be overridden by the user, otherwise it will
	 * not remove the oldest object from the Map.
	 */
	@Override
	protected boolean removeEldestEntry( Map.Entry< K, V > eldest ) {
		return size() > this.capacity;
	}
}