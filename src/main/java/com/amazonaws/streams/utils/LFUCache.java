package com.amazonaws.streams.utils;

import java.util.*;

public class LFUCache {
    HashMap<Integer, Integer> vals;//cache K and V
    HashMap<Integer, Integer> counts;//K and counters
    HashMap<Integer, LinkedHashSet<Integer>> lists;//Counter and item list
    int cap;
    int min = -1;

    public LFUCache(int capacity) {
        cap = capacity;
        vals = new HashMap<>();
        counts = new HashMap<>();
        lists = new HashMap<>();
        lists.put(1, new LinkedHashSet<>());
    }

    public int get(int key) {
        if (!vals.containsKey(key)) {
            //Load it from Actual Store (Redis?) and change the line below
            int value = -1;
            put(key, value);
            return value;
        }

        // Get the count from counts map
        int count = counts.get(key);
        // increase the counter
        counts.put(key, count + 1);
        // remove the element from the counter to linkedhashset
        lists.get(count).remove(key);

        // when current min does not have any data, next one would be the min
        if (count == min && lists.get(count).size() == 0)
            min++;
        if (!lists.containsKey(count + 1))
            lists.put(count + 1, new LinkedHashSet<>());
        lists.get(count + 1).add(key);
        return vals.get(key);
    }

    public void put(int key, int value) {
        if (cap <= 0)
            return;
        // If key does exist, we are returning from here
        if (vals.containsKey(key)) {
            vals.put(key, value);
            get(key);
            return;
        }
        if (vals.size() >= cap) {
            int evit = lists.get(min).iterator().next();
            lists.get(min).remove(evit);
            vals.remove(evit);
            counts.remove(evit);       
        }
        // If the key is new, insert the value and current min should be 1 of course
        vals.put(key, value);
        counts.put(key, 1);
        min = 1;
        lists.get(1).add(key);
    }
}