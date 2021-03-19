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

import com.dslplatform.json.JsonWriter;

import static com.dslplatform.json.JsonWriter.SEMI;

public final class JsonWriteHelpers {
	public static void startObject(JsonWriter writer) {
		writer.writeByte(JsonWriter.OBJECT_START);
	}

	public static void endObject(JsonWriter writer) {
		writer.writeByte(JsonWriter.OBJECT_END);
	}

	public static <T> JsonWriter.WriteObject<T> typedWriter(){
		return (writer, value) -> {
			if (value == null) {
				writer.writeNull();
				return;
			}
			startObject(writer);
			writer.writeString(value.getClass().getSimpleName());
			writer.writeByte(SEMI);
			writer.serializeObject(value);
			endObject(writer);
		};
	}
}
