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

package io.activej.fs.util;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.collection.Try;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnexpectedDataException;
import io.activej.common.ref.RefLong;
import io.activej.common.reflection.TypeT;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.fs.exception.FsBatchException;
import io.activej.fs.exception.FsException;
import io.activej.fs.exception.FsIOException;
import io.activej.fs.exception.FsScalarException;
import io.activej.promise.Promise;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.json.JsonUtils.fromJson;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RemoteFsUtils {
	private static final Pattern ANY_GLOB_METACHARS = Pattern.compile("[*?{}\\[\\]\\\\]");
	private static final Pattern UNESCAPED_GLOB_METACHARS = Pattern.compile("(?<!\\\\)(?:\\\\\\\\)*[*?{}\\[\\]]");

	/**
	 * Escapes any glob metacharacters so that given path string can ever only match one file.
	 *
	 * @param path path that potentially can contain glob metachars
	 * @return escaped glob which matches only a file with that name
	 */
	public static String escapeGlob(String path) {
		return ANY_GLOB_METACHARS.matcher(path).replaceAll("\\\\$0");
	}

	/**
	 * Checks if given glob can match more than one file.
	 *
	 * @param glob the glob to check.
	 * @return <code>true</code> if given glob can match more than one file.
	 */
	public static boolean isWildcard(String glob) {
		return UNESCAPED_GLOB_METACHARS.matcher(glob).find();
	}

	/**
	 * Returns a {@link PathMatcher} for given glob
	 *
	 * @param glob a glob string
	 * @return a path matcher for the glob string
	 */
	public static PathMatcher getGlobPathMatcher(String glob) {
		return FileSystems.getDefault().getPathMatcher("glob:" + glob);
	}

	/**
	 * Same as {@link #getGlobPathMatcher(String)} but returns a string predicate.
	 *
	 * @param glob a glob string
	 * @return a predicate for the glob string
	 */
	public static Predicate<String> getGlobStringPredicate(String glob) {
		PathMatcher matcher = getGlobPathMatcher(glob);
		return str -> matcher.matches(Paths.get(str));
	}

	public static <T> Function<ByteBuf, Promise<T>> decodeBody(Class<T> cls) {
		return body -> {
			try {
				return Promise.of(fromJson(cls, body.getString(UTF_8)));
			} catch (MalformedDataException e) {
				return Promise.ofException(e);
			}
		};
	}

	public static <T> Function<ByteBuf, Promise<T>> decodeBody(TypeT<? extends T> type) {
		return body -> {
			try {
				return Promise.of(fromJson(type, body.getString(UTF_8)));
			} catch (MalformedDataException e) {
				return Promise.ofException(e);
			}
		};
	}

	public static ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> ofFixedSize(long size) {
		return consumer -> {
			RefLong total = new RefLong(size);
			return consumer
					.<ByteBuf>mapAsync(byteBuf -> {
						long left = total.dec(byteBuf.readRemaining());
						if (left < 0) {
							byteBuf.recycle();
							return Promise.ofException(new UnexpectedDataException());
						}
						return Promise.of(byteBuf);
					})
					.withAcknowledgement(ack -> ack
							.then(() -> {
								if (total.get() > 0) {
									return Promise.ofException(new TruncatedDataException());
								}
								return Promise.complete();
							}));
		};
	}

	/*
		Casting received errors before sending to remote peer
		ActiveFs.class is used as a component to hide implementation details from peer
	 */
	public static FsException castError(Throwable e) {
		return e instanceof FsException ? (FsException) e : new FsIOException("Unknown error");
	}

	public static FsBatchException batchEx(String name, FsScalarException exception) {
		return new FsBatchException(map(name, exception));
	}

	public static Promise<Void> reduceErrors(List<Try<Void>> tries, Iterator<String> sources) {
		Map<String, FsScalarException> scalarExceptions = new HashMap<>();
		for (Try<Void> aTry : tries) {
			String source = sources.next();
			if (aTry.isSuccess()) continue;
			Throwable exception = aTry.getException();
			if (exception instanceof FsScalarException) {
				scalarExceptions.put(source, ((FsScalarException) exception));
			} else {
				return Promise.ofException(exception);
			}
		}
		if (!scalarExceptions.isEmpty()) {
			return Promise.ofException(new FsBatchException(scalarExceptions));
		}
		return Promise.complete();
	}

}
