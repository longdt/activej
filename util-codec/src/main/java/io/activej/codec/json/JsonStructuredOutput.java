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

package io.activej.codec.json;

import com.dslplatform.json.BinaryConverter;
import com.dslplatform.json.BoolConverter;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.NumberConverter;
import io.activej.codec.StructuredCodecs;
import io.activej.codec.StructuredEncoder;
import io.activej.codec.StructuredOutput;
import io.activej.common.Checks;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkState;

public class JsonStructuredOutput implements StructuredOutput {
	private static final boolean CHECK = Checks.isEnabled(JsonStructuredOutput.class);

	private final JsonWriter writer;

	private final BitSet nestedBitSet = new BitSet(0);
	private int bitSetIndex = -2;

	/**
	 * Constructs a new {@link JsonStructuredOutput}
	 * Passed {@link JsonWriter} should not perform any blocking I/O operations
	 *
	 * @param writer nonblocking {@link JsonWriter}
	 */
	public JsonStructuredOutput(JsonWriter writer) {
		this.writer = writer;
	}

	@Override
	public void writeBoolean(boolean value) {
		beforeWrite();
		BoolConverter.serialize(value, writer);
	}

	@Override
	public void writeByte(byte value) {
		beforeWrite();
		writer.writeByte(value);
	}

	@Override
	public void writeInt(int value) {
		beforeWrite();
		NumberConverter.serialize(value, writer);
	}

	@Override
	public void writeLong(long value) {
		beforeWrite();
		NumberConverter.serialize(value, writer);
	}

	@Override
	public void writeInt32(int value) {
		beforeWrite();
		NumberConverter.serialize(value, writer);
	}

	@Override
	public void writeLong64(long value) {
		beforeWrite();
		NumberConverter.serialize(value, writer);
	}

	@Override
	public void writeFloat(float value) {
		beforeWrite();
		NumberConverter.serialize(value, writer);
	}

	@Override
	public void writeDouble(double value) {
		beforeWrite();
		NumberConverter.serialize(value, writer);
	}

	@Override
	public void writeBytes(byte[] bytes, int off, int len) {
		beforeWrite();
		BinaryConverter.serialize(Arrays.copyOfRange(bytes, off, off + len), writer);
	}

	@Override
	public void writeBytes(byte[] bytes) {
		beforeWrite();
		BinaryConverter.serialize(bytes, writer);
	}

	@Override
	public void writeString(String value) {
		beforeWrite();
		writer.writeString(value);
	}

	@Override
	public void writeNull() {
		beforeWrite();
		writer.writeNull();
	}

	@Override
	public <T> void writeNullable(StructuredEncoder<T> encoder, T value) {
		if (value != null) {
			encoder.encode(this, value);
		} else {
			writeNull();
		}
	}

	@Override
	public <T> void writeList(StructuredEncoder<T> encoder, List<T> list) {
		beforeWrite();
		beginArray();
		for (T item : list) {
			encoder.encode(this, item);
		}
		endArray();
	}

	@Override
	public <K, V> void writeMap(StructuredEncoder<K> keyEncoder, StructuredEncoder<V> valueEncoder, Map<K, V> map) {
		beforeWrite();
		if (keyEncoder == StructuredCodecs.STRING_CODEC) {
			beginObject();
			for (Map.Entry<K, V> entry : map.entrySet()) {
				writeKey((String) entry.getKey());
				valueEncoder.encode(this, entry.getValue());
			}
			endObject();
		} else {
			beginArray();
			for (Map.Entry<K, V> entry : map.entrySet()) {
				beforeWrite();
				beginArray();
				keyEncoder.encode(this, entry.getKey());
				valueEncoder.encode(this, entry.getValue());
				endArray();
			}
			endArray();
		}
	}

	@Override
	public <T> void writeTuple(StructuredEncoder<T> encoder, T value) {
		beforeWrite();
		beginArray();
		encoder.encode(this, value);
		endArray();
	}

	@Override
	public <T> void writeObject(StructuredEncoder<T> encoder, T value) {
		beforeWrite();
		beginObject();
		encoder.encode(this, value);
		endObject();
	}

	@Override
	public void writeKey(String field) {
		if (CHECK) checkState(bitSetIndex >= 0 && nestedBitSet.get(bitSetIndex), "Writing keys outside object");

		int index = bitSetIndex + 1;
		if (nestedBitSet.get(index)) {
			writer.writeByte(JsonWriter.COMMA);
		} else {
			nestedBitSet.set(index);
		}

		writer.writeString(field);
		writer.writeByte(JsonWriter.SEMI);
	}

	@Override
	public <T> void writeCustom(Type type, T value) {
		throw new UnsupportedOperationException("No custom type writers");
	}

	private void beforeWrite() {
		if (bitSetIndex >= 0) {
			if (!nestedBitSet.get(bitSetIndex)) {
				int index = bitSetIndex + 1;
				if (nestedBitSet.get(index)) {
					writer.writeByte(JsonWriter.COMMA);
				} else {
					nestedBitSet.set(index);
				}
			}
		}
	}

	private void beginArray() {
		writer.writeByte(JsonWriter.ARRAY_START);
		bitSetIndex += 2;
	}

	private void endArray() {
		writer.writeByte(JsonWriter.ARRAY_END);
		nestedBitSet.clear(bitSetIndex + 1);
		bitSetIndex -= 2;
	}

	private void beginObject() {
		writer.writeByte(JsonWriter.OBJECT_START);
		bitSetIndex += 2;
		nestedBitSet.set(bitSetIndex);
	}

	private void endObject() {
		writer.writeByte(JsonWriter.OBJECT_END);
		nestedBitSet.clear(bitSetIndex, bitSetIndex + 2);
		bitSetIndex -= 2;
	}
}
