package delta.java;

import java.util.*;
import java.util.function.BiFunction;

/**
 * Convenience class for dealing with immutable
 * collections in Java.
 * NOTE: This class does basic ease-of-use
 * functionality; it doesn't do any verification.
 * Also, specialty collections, such as sorted
 * collections, and other implementation details
 * are happily ignored.
 */
public class Immutable {
    public static <K, V> Map<K, V> remove(Map<K, V> map, K key) {
        if (map.containsKey(key)) {
            HashMap<K, V> newMap = new HashMap<>(map);
            newMap.remove(key);
            return Collections.unmodifiableMap(newMap);
        } else {
            return map;
        }
    }

    public static <K, V> Map<K, V> put(Map<K, V> map, K key, V value) {
        HashMap<K, V> newMap = new HashMap<>(map);
        newMap.put(key, value);
        return Collections.unmodifiableMap(newMap);
    }

    public static <K, V> Map<K, V> merge(Map<K, V> map, K key, V value, BiFunction<? super V, ? super V, ? extends V> merge) {
        HashMap<K, V> newMap = new HashMap<>(map);
        newMap.merge(key, value, merge);
        return Collections.unmodifiableMap(newMap);
    }

    public static <E> List<E> remove(List<E> list, E removeElem) {
        ArrayList<E> newList = new ArrayList<>(list.size());
        for (E elem : list) {
            if (!elem.equals(removeElem)) {
                newList.add(elem);
            }
        }
        return Collections.unmodifiableList(newList);
    }

    public static <E> List<E> add(List<E> list, E elem) {
        ArrayList<E> newList = new ArrayList<>(list);
        newList.add(elem);
        return Collections.unmodifiableList(newList);
    }

    public static <E> Set<E> remove(Set<E> set, E removeElem) {
        if (set.contains(removeElem)) {
            HashSet<E> newSet = new HashSet<>(set);
            newSet.remove(removeElem);
            return Collections.unmodifiableSet(newSet);
        } else {
            return set;
        }
    }
    public static <E> Set<E> add(Set<E> set, E elem) {
        if (set.contains(elem)) {
            return set;
        } else {
            HashSet<E> newSet = new HashSet<>(set);
            newSet.add(elem);
            return Collections.unmodifiableSet(newSet);
        }
    }
    private static boolean isEffectivelyImmutable(Object coll) {
        String name = coll.getClass().getName();
        return name.contains("Immutable") || name.contains("Unmodifiable");
    }
    @SafeVarargs
    public static <E> List<E> list(E... content) {
        return Collections.unmodifiableList(Arrays.asList(content));
    }
    @SafeVarargs
    public static <E> Set<E> set(E... content) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(content)));
    }
    public static <E> List<E> ensure(List<E> list) {
        if (isEffectivelyImmutable(list)) return list;
        else return Collections.unmodifiableList(new ArrayList<>(list));
    }
    public static <E> Set<E> ensure(Set<E> set) {
        if (isEffectivelyImmutable(set)) return set;
        else return Collections.unmodifiableSet(new HashSet<>(set));
    }
    public static <K, V> Map<K, V> ensure(Map<K, V> map) {
        if (isEffectivelyImmutable(map)) return map;
        else return Collections.unmodifiableMap(new HashMap<>(map));
    }

}
