package io.activej.crdt.wal;

import io.activej.promise.Promise;

public class NoopWriteAheadLog<K extends Comparable<K>, S> implements WriteAheadLog<K, S> {
	@Override
	public Promise<Void> put(K key, S value) {
		return Promise.complete();
	}

	@Override
	public Promise<Void> flush() {
		return Promise.complete();
	}
}
