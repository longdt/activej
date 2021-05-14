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

package io.activej.launchers.crdt;

import io.activej.common.reflection.TypeT;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.util.CrdtDataSerializer;

public final class CrdtDescriptor<K extends Comparable<K>, S> {
	private final CrdtFunction<S> crdtFunction;
	private final CrdtDataSerializer<K, S> serializer;
	private final TypeT<K> keyManifest;
	private final TypeT<S> stateManifest;

	public CrdtDescriptor(CrdtFunction<S> crdtFunction, CrdtDataSerializer<K, S> serializer, TypeT<K> keyManifest, TypeT<S> stateManifest) {
		this.crdtFunction = crdtFunction;
		this.serializer = serializer;
		this.keyManifest = keyManifest;
		this.stateManifest = stateManifest;
	}

	public CrdtDescriptor(CrdtFunction<S> crdtFunction, CrdtDataSerializer<K, S> serializer, Class<K> keyManifest, Class<S> stateManifest) {
		this(crdtFunction, serializer, TypeT.of(keyManifest), TypeT.of(stateManifest));
	}

	public CrdtFunction<S> getCrdtFunction() {
		return crdtFunction;
	}

	public CrdtDataSerializer<K, S> getSerializer() {
		return serializer;
	}

	public TypeT<K> getKeyManifest() {
		return keyManifest;
	}

	public TypeT<S> getStateManifest() {
		return stateManifest;
	}
}
