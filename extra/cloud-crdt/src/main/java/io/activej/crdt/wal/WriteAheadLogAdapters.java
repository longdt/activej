package io.activej.crdt.wal;

import io.activej.promise.Promise;

import static io.activej.common.Checks.checkArgument;

public final class WriteAheadLogAdapters {

	public static <K extends Comparable<K>, S> WriteAheadLog<K, S> flushOnUpdatesCount(WriteAheadLog<K, S> original, int updatesCount) {
		checkArgument(updatesCount > 0);

		return new ForwardingWriteAheadLog<K, S>(original) {
			private int count;

			@Override
			public Promise<Void> put(K key, S value) {
				return super.put(key, value)
						.then(() -> ++count == updatesCount ? flush() : Promise.complete());
			}
		};
	}
}
