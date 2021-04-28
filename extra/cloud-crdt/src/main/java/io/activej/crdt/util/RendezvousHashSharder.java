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

package io.activej.crdt.util;

import io.activej.common.ApplicationSettings;
import io.activej.common.HashUtils;
import io.activej.common.ref.RefInt;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;

public final class RendezvousHashSharder {
	public static final int NUMBER_OF_BUCKETS = ApplicationSettings.getInt(RendezvousHashSharder.class, "numberOfBuckets", 1024);

	static {
		checkArgument((NUMBER_OF_BUCKETS & (NUMBER_OF_BUCKETS - 1)) == 0, "Number of buckets must be a power of two");
	}

	private int[][] buckets;

	private RendezvousHashSharder() {
	}

	public static RendezvousHashSharder create(Set<? extends Comparable<?>> partitionIds, int topShards) {
		RendezvousHashSharder sharder = new RendezvousHashSharder();
		sharder.recompute(partitionIds, topShards);
		return sharder;
	}

	public void recompute(Set<? extends Comparable<?>> partitionIds, int topShards) {
		checkArgument(topShards > 0, "Top number of partitions must be positive");
		checkArgument(topShards <= partitionIds.size(), "Top number of partitions must be less than or equal to number of partitions");

		buckets = new int[NUMBER_OF_BUCKETS][topShards];

		class ObjWithIndex<O> {
			final O object;
			final int index;

			ObjWithIndex(O object, int index) {
				this.object = object;
				this.index = index;
			}
		}
		RefInt indexRef = new RefInt(0);
		List<ObjWithIndex<Object>> indexed = partitionIds.stream()
				.sorted()
				.map(partitionId -> new ObjWithIndex<>((Object) partitionId, indexRef.value++))
				.collect(toList());

		for (int n = 0; n < buckets.length; n++) {
			int finalN = n;
			List<ObjWithIndex<Object>> copy = indexed.stream()
					.sorted(Comparator.<ObjWithIndex<Object>>comparingInt(x -> HashUtils.murmur3hash(x.object.hashCode(), finalN)).reversed())
					.collect(toList());

			for (int i = 0; i < topShards; i++) {
				buckets[n][i] = copy.get(i).index;
			}
		}
	}

	public int[] shard(Object key) {
		return buckets[key.hashCode() & (NUMBER_OF_BUCKETS - 1)];
	}
}
