package com.mikeldeltio.kafka.util;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;

public class MapUtils {

	public static <K, V> Entry<K, V> entry(K key, V value) {
		return new SimpleImmutableEntry<>(key, value);
	}

	@SafeVarargs
	public static <K, V> Map<K, V> createMap(Entry<K, V>... data) {
		return stream(data).collect(toMap(e -> e.getKey(), e -> e.getValue()));
	}

}
