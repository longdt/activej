/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.json;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.SEMI;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class JsonReadHelpers {

	public static void startObject(JsonReader<?> reader) throws IOException {
		if (reader.last() != JsonWriter.OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();
	}

	public static void endObject(JsonReader<?> reader) throws IOException {
		if (reader.last() != JsonWriter.OBJECT_END) throw reader.newParseError("Expected '}'");
	}

	public static <T> T readValue(JsonReader<?> reader, String key, JsonReader.ReadObject<T> readObject) throws IOException {
		String readKey = reader.readKey();
		if (!readKey.equals(key)) throw reader.newParseError("Expected key '" + key + '\'');
		T t = readObject.read(reader);
		byte next = reader.getNextToken();
		if (next == ',') {
			reader.getNextToken();
		} else if (next != '}') {
			throw reader.newParseError("Malformed JSON object: " + reader);
		}
		return t;
	}

	@SafeVarargs
	public static <T> JsonReader.ReadObject<T> typedReader(Class<? extends T> ... types){
		Map<String, Class<? extends T>> typeMap = Arrays.stream(types)
				.collect(toMap(Class::getSimpleName, identity()));

		return reader -> {
			if (reader.wasNull()) return null;
			startObject(reader);

			String type = reader.readString();
			if (reader.getNextToken() != SEMI) {
				throw ParsingException.create("':' expected after object key", true);
			}

			T result;

			Class<? extends T> aClass = typeMap.get(type);
			try {
				//noinspection unchecked
				result = (T) reader.next(aClass);
			} catch (IllegalArgumentException ignored) {
				throw ParsingException.create("Unknown type: " + type, true);
			}
			reader.getNextToken();
			endObject(reader);

			return result;
		};
	}
}
