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

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.Nullable;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.reflection.TypeT;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public final class JsonUtils {
	public static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	public static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);

	public static <T> String toJson(@Nullable T object) {
		if (object == null) return "null";
		//noinspection unchecked
		return toJson((Class<? super T>) object.getClass(), object);
	}

	public static <T> ByteBuf toJsonBuf(@Nullable T object) {
		if (object == null) return ByteBuf.wrap(new byte[]{'n', 'u', 'l', 'l'}, 0, 4);
		return toJsonBuf(object.getClass(), object);
	}

	public static <T> String toJson(@NotNull TypeT<? super T> manifest, @Nullable T object) {
		return toJson(manifest.getType(), object).toString();
	}

	public static <T> ByteBuf toJsonBuf(@NotNull TypeT<? super T> manifest, @Nullable T object) {
		return toJsonBuf(manifest.getType(), object);
	}

	public static <T> String toJson(@NotNull Class<? super T> manifest, @Nullable T object) {
		return toJson((Type) manifest, object).toString();
	}

	public static <T> ByteBuf toJsonBuf(@NotNull Class<? super T> manifest, @Nullable T object) {
		return toJsonBuf((Type) manifest, object);
	}

	private static <T> ByteBuf toJsonBuf(@NotNull Type manifest, @Nullable T object) {
		JsonWriter writer = toJson(manifest, object);
		return ByteBuf.wrapForReading(writer.toByteArray());
	}

	private static <T> JsonWriter toJson(@NotNull Type manifest, @Nullable T object) {
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		if (!DSL_JSON.serialize(jsonWriter, manifest, object)) {
			throw new IllegalArgumentException("Cannot serialize " + manifest);
		}
		return jsonWriter;
	}

	public static <T> T fromJson(@NotNull TypeT<? extends T> manifest, @NotNull String jsonString) throws MalformedDataException {
		return fromJson(manifest.getType(), jsonString);
	}

	public static <T> T fromJson(@NotNull Class<? extends T> manifest, @NotNull String jsonString) throws MalformedDataException {
		return fromJson((Type) manifest, jsonString);
	}

	@SuppressWarnings("unchecked")
	private static <T> T fromJson(@NotNull Type manifest, @NotNull String jsonString) throws MalformedDataException {
		byte[] bytes = jsonString.getBytes(StandardCharsets.UTF_8);
		try {
			Object deserialized = DSL_JSON.deserialize(manifest, bytes, bytes.length);
			return (T) deserialized;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e){
			throw new AssertionError(e);
		}
	}
}
