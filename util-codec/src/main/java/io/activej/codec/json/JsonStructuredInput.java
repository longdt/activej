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

import com.dslplatform.json.*;
import io.activej.codec.StructuredDecoder;
import io.activej.codec.StructuredInput;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UncheckedException;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static io.activej.codec.StructuredCodecs.STRING_CODEC;
import static io.activej.codec.StructuredInput.Token.*;

public final class JsonStructuredInput implements StructuredInput {
	private final JsonReader<?> reader;
	private int nested = 0;

	/**
	 * Constructs a new {@link JsonStructuredInput}
	 * Passed {@link JsonReader} should not perform any blocking I/O operations
	 *
	 * @param reader nonblocking {@link JsonReader}
	 */
	public JsonStructuredInput(JsonReader<?> reader) {
		this.reader = reader;
	}

	@Override
	public boolean readBoolean() throws MalformedDataException {
		try {
			boolean result = BoolConverter.deserialize(reader);
			afterRead();
			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public byte readByte() throws MalformedDataException {
		int n = readInt();
		if (n != (n & 0xFF)) throw new MalformedDataException("Expected byte, but was: " + n);
		afterRead();
		return (byte) n;
	}

	@Override
	public int readInt() throws MalformedDataException {
		try {
			int result = NumberConverter.deserializeInt(reader);
			afterRead();
			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public long readLong() throws MalformedDataException {
		try {
			long result = NumberConverter.deserializeLong(reader);
			afterRead();
			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public int readInt32() throws MalformedDataException {
		return readInt();
	}

	@Override
	public long readLong64() throws MalformedDataException {
		return readLong();
	}

	@Override
	public float readFloat() throws MalformedDataException {
		try {
			float result = NumberConverter.deserializeFloat(reader);
			afterRead();
			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public double readDouble() throws MalformedDataException {
		try {
			double result = NumberConverter.deserializeDouble(reader);
			afterRead();
			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public byte[] readBytes() throws MalformedDataException {
		try {
			byte[] result = BinaryConverter.deserialize(reader);
			afterRead();
			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public String readString() throws MalformedDataException {
		try {
			String result = reader.readString();
			afterRead();
			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void readNull() throws MalformedDataException {
		try {
			if (!reader.wasNull()) {
				throw new MalformedDataException("Was not 'null'");
			}
			afterRead();
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		}
	}

	@Override
	public <T> T readNullable(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			if (reader.wasNull()) {
				afterRead();
				return null;
			}
			return decoder.decode(this);
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		}
	}

	@Override
	public <T> T readTuple(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			beginArray();
			reader.getNextToken();
			nested++;

			T result = decoder.decode(this);
			reader.checkArrayEnd();

			afterRead();

			return result;
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	private void beginArray() throws MalformedDataException {
		if (reader.last() != '[') {
			throw new MalformedDataException("JSON array expected");
		}
	}

	private void beginObject() throws MalformedDataException {
		if (reader.last() != '{') {
			throw new MalformedDataException("JSON object expected");
		}
	}

	@Override
	public <T> T readObject(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			beginObject();
			nested++;
			reader.getNextToken();

			T result = decoder.decode(this);

			reader.checkObjectEnd();
			afterRead();

			return result;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> List<T> readList(StructuredDecoder<T> decoder) throws MalformedDataException {
		try {
			List<T> list = new ArrayList<>();
			beginArray();
			if (reader.getNextToken() == ']') return Collections.emptyList();

			int i = ++nested;

			while (nested >= i) {
				list.add(decoder.decode(this));
			}

			afterRead();

			return list;
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<K, V> readMap(StructuredDecoder<K> keyDecoder, StructuredDecoder<V> valueDecoder) throws
			MalformedDataException {
		try {
			if (keyDecoder == STRING_CODEC) {
				beginObject();

				if (reader.getNextToken() == ']') return Collections.emptyMap();

				Map<K, V> map = new LinkedHashMap<>();

				int i = ++nested;
				while (nested >= i) {
					K key = (K) reader.readKey();
					V value = valueDecoder.decode(this);
					map.put(key, value);
				}
				reader.checkObjectEnd();
				return map;
			} else {
				Map<K, V> map = new LinkedHashMap<>();
				reader.startArray();
//				while (reader.hasNext()) {
				reader.startArray();
				K key = keyDecoder.decode(this);
				V value = valueDecoder.decode(this);
				map.put(key, value);
				reader.endArray();
//				}
				reader.endArray();
				return map;
			}
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public boolean hasNext() throws MalformedDataException {
		byte token = reader.last();
		return token != JsonWriter.ARRAY_END && token != JsonWriter.OBJECT_END;
	}

	@Override
	public String readKey() throws MalformedDataException {
		try {
			return reader.readKey();
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	@SuppressWarnings("RedundantThrows")
	public <T> T readCustom(Type type) throws MalformedDataException {
		throw new UnsupportedOperationException("No custom type readers");
	}

	@Override
	public EnumSet<Token> getNext() throws MalformedDataException {
		byte last = reader.last();

		switch (last) {
			case 'n':
				return EnumSet.of(NULL);
			case 't':
			case 'f':
				return EnumSet.of(BOOLEAN);
			case '"':
				return EnumSet.of(STRING, BYTES);
			case '[':
				return EnumSet.of(LIST, TUPLE);
			case '{':
				return EnumSet.of(MAP, OBJECT);
			default:
				return EnumSet.of(BYTE, INT, LONG, FLOAT, DOUBLE);
		}
	}

	private void afterRead() throws MalformedDataException {
		if (nested > 0) {
			try {
				byte last = reader.getNextToken();
				if (last == ',') {
					reader.getNextToken();
				} else if (last == ']' || last == '}') {
					nested--;
				} else {
					throw new MalformedDataException("Unexpected token: " + (char) last);
				}
			} catch (ParsingException e) {
				throw new MalformedDataException(e);
			} catch (IOException e) {
				throw new AssertionError();
			}
		}
	}
}
