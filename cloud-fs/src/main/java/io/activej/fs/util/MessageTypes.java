package io.activej.fs.util;

import io.activej.common.reflection.TypeT;
import io.activej.fs.FileMetadata;

import java.util.Map;
import java.util.Set;

public final class MessageTypes {
	public static final TypeT<Set<String>> STRING_SET_TYPE = new TypeT<Set<String>>() {};
	public static final TypeT<Map<String, String>> STRING_STRING_MAP_TYPE = new TypeT<Map<String, String>>() {};
	public static final TypeT<Map<String, FileMetadata>> STRING_META_MAP_TYPE = new TypeT<Map<String, FileMetadata>>() {};

}
