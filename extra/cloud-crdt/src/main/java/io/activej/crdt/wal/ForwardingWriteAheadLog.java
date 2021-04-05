package io.activej.crdt.wal;

import io.activej.promise.Promise;

public abstract class ForwardingWriteAheadLog<K extends Comparable<K>, S> implements WriteAheadLog<K, S> {
	private final WriteAheadLog<K, S> peer;

	public ForwardingWriteAheadLog(WriteAheadLog<K, S> peer) {
		this.peer = peer;
	}

	@Override
	public Promise<Void> put(K key, S value) {
		return peer.put(key, value);
	}

	@Override
	public Promise<Void> flush() {
		return peer.flush();
	}
}
