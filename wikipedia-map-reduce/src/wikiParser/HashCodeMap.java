package wikiParser;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author colin
 */
public class HashCodeMap {

	private LinkedHashCodeMap<Integer, Revision> hashMap;
	private int maxEntries;

	public HashCodeMap(int maxEntries) {
		this.maxEntries = maxEntries;
		this.hashMap = new LinkedHashCodeMap<Integer, Revision>(maxEntries);
	}

	public Revision get(Integer hash) {
		return this.hashMap.get(hash);
	}

	public void add(Integer hash, Revision revision) {
		if (this.hashMap.get(hash) == null) {
			this.hashMap.put(hash, revision);
		}
	}

	public HashMap<Integer, Revision> getHashMap() {
		return this.hashMap;
	}

	public void addAll(HashCodeMap h) {
		for (Integer k : h.getHashMap().keySet()) {
			this.hashMap.put(k, h.get(k));
		}
	}

	public int size() {
		return this.hashMap.size();
	}

	public boolean isKeyValuePair(Integer hashcode, Revision revision) {
		Revision r = this.hashMap.get(new Integer(hashcode));
		return (r != null && revision.equals(r));
	}

	public boolean isNew(Integer hashcode) {
		return (!this.hashMap.containsKey(hashcode));
	}

	private class LinkedHashCodeMap<K, V> extends LinkedHashMap<K, V> {

		private static final long serialVersionUID = 1L;

		protected LinkedHashCodeMap(int initialCapacity) {
			super(initialCapacity);
		}

		protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
			return size() > maxEntries;
		}
	}
}
