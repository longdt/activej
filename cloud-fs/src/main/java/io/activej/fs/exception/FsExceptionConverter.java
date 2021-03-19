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

package io.activej.fs.exception;

import com.dslplatform.json.JsonConverter;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.OBJECT_END;
import static com.dslplatform.json.JsonWriter.SEMI;
import static io.activej.json.JsonReadHelpers.endObject;
import static io.activej.json.JsonReadHelpers.readValue;
import static io.activej.json.JsonReadHelpers.startObject;
import static io.activej.json.JsonWriteHelpers.endObject;
import static io.activej.json.JsonWriteHelpers.startObject;

@SuppressWarnings({"unused"})
@JsonConverter(target = FsException.class)
public abstract class FsExceptionConverter {
	private static final String MESSAGE = "message";
	private static final String EXCEPTIONS = "exceptions";

	public static final JsonReader.ReadObject<FsException> JSON_READER = FsExceptionConverter::readFsException;

	public static final JsonWriter.WriteObject<FsException> JSON_WRITER = FsExceptionConverter::writeFsException;

	private static FsException readFsException(JsonReader<?> reader) throws IOException {
		if (reader.wasNull()) return null;
		startObject(reader);

		String type = reader.readKey();
		startObject(reader);

		FsException exception;

		if (type.equals("FsBatchException")) {
			Map<String, FsScalarException> exceptions = readValue(reader, EXCEPTIONS, FsExceptionConverter::readExceptions);
			exception = new FsBatchException(exceptions, false);
		} else {
			String message = readValue(reader, MESSAGE, JsonReader::readString);
			switch (type) {
				case "FsException":
					exception = new FsException(message, false);
					break;
				case "FileNotFoundException":
					exception = new FileNotFoundException(message, false);
					break;
				case "ForbiddenPathException":
					exception = new ForbiddenPathException(message, false);
					break;
				case "FsIOException":
					exception = new FsIOException(message, false);
					break;
				case "FsScalarException":
					exception = new FsScalarException(message, false);
					break;
				case "FsStateException":
					exception = new FsStateException(message, false);
					break;
				case "IllegalOffsetException":
					exception = new IllegalOffsetException(message, false);
					break;
				case "IsADirectoryException":
					exception = new IsADirectoryException(message, false);
					break;
				case "MalformedGlobException":
					exception = new MalformedGlobException(message, false);
					break;
				case "PathContainsFileException":
					exception = new PathContainsFileException(message, false);
					break;
				default:
					throw ParsingException.create("Unknown type: " + type, true);
			}
		}
		endObject(reader);
		reader.getNextToken();
		endObject(reader);
		return exception;
	}

	private static void writeFsException(JsonWriter writer, FsException value) {
		if (value == null) {
			writer.writeNull();
			return;
		}
		startObject(writer);
		writer.writeString(value.getClass().getSimpleName());
		writer.writeByte(SEMI);
		startObject(writer);
		if (value instanceof FsBatchException) {
			writer.writeString(EXCEPTIONS);
			writer.writeByte(SEMI);
			writeExceptions(writer, ((FsBatchException) value).getExceptions());
		} else {
			writer.writeString(MESSAGE);
			writer.writeByte(SEMI);
			writer.writeString(value.getMessage());
		}
		endObject(writer);
		endObject(writer);
	}

	private static Map<String, FsScalarException> readExceptions(JsonReader<?> reader) throws IOException {
		startObject(reader);
		if (reader.last() == OBJECT_END) return Collections.emptyMap();
		Map<String, FsScalarException> res = new LinkedHashMap<>();
		String key = reader.readKey();
		res.put(key, readScalarException(reader));
		while (reader.getNextToken() == ',') {
			reader.getNextToken();
			key = reader.readKey();
			res.put(key, readScalarException(reader));
		}
		if (reader.last() != OBJECT_END) throw reader.newParseError("Expected '}'");
		return res;
	}

	private static void writeExceptions(JsonWriter writer, Map<String, FsScalarException> exceptions) {
		writer.writeByte(JsonWriter.OBJECT_START);
		if (!exceptions.isEmpty()) {
			boolean isFirst = true;

			for (Map.Entry<String, FsScalarException> entry : exceptions.entrySet()) {
				if (!isFirst) writer.writeByte(JsonWriter.COMMA);
				isFirst = false;
				writer.writeString(entry.getKey());
				writer.writeByte(SEMI);
				writeFsException(writer, entry.getValue());
			}
		}
		writer.writeByte(OBJECT_END);
	}

	@NotNull
	private static FsScalarException readScalarException(JsonReader<?> reader) throws IOException {
		FsException exception = readFsException(reader);
		if (!(exception instanceof FsScalarException)) {
			throw ParsingException.create("Expected exception to be instance of FsScalarException", true);
		}
		return (FsScalarException) exception;
	}
}
