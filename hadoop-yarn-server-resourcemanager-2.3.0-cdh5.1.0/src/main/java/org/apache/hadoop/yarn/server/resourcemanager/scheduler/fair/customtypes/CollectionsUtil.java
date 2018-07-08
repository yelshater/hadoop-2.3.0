package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.customtypes;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class CollectionsUtil {
	
	public static <T> void incrementMapValue (Map<T, Integer> map , T key) {
		if (map == null)
			return;
		if (map.containsKey(key)) {
			Integer value = map.get(key);
			map.put(key, value.intValue() + 1);
		}
		else {
			map.put(key, 1);
		}
	}
	
	public static <T> void decrementMapValue (Map<T, Integer> map , T key) {
		if (map == null)
			return;
		if (map.containsKey(key)) {
			Integer value = map.get(key);
			map.put(key, value.intValue() - 1);
		}
		else {
			map.put(key, 0);
		}
	}
	
	
	
	public static Map<String,Integer> sortMapByValues (Map<String , Integer> map) {
		 ValueComparator bvc =  new ValueComparator(map);
	     Map<String,Integer> sortedMap = new TreeMap<String,Integer>(bvc);
	     sortedMap.putAll(map);
	     return sortedMap;
	}
	
}

class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        }
    }
}
