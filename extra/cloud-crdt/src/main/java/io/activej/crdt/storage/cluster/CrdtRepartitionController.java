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

package io.activej.crdt.storage.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.process.AsyncCloseable;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.EventStats;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

public final class CrdtRepartitionController<K extends Comparable<K>, S> implements EventloopJmxBeanEx {
	private final Comparable<?> localPartitionId;
	private final CrdtStorage<K, S> localClient;
	private final CrdtStorageCluster<K, S> cluster;

	private final AsyncSupplier<Void> repartition = AsyncSuppliers.reuse(this::doRepartition);

	public CrdtRepartitionController(Comparable<?> localPartitionId, CrdtStorage<K, S> localClient, CrdtStorageCluster<K, S> cluster) {
		this.localClient = localClient;
		this.cluster = cluster;
		this.localPartitionId = localPartitionId;
	}

	public static <I extends Comparable<I>, K extends Comparable<K>, S> CrdtRepartitionController<K, S> create(CrdtStorageCluster<K, S> cluster, I localPartitionId) {
		return new CrdtRepartitionController<>(localPartitionId, cluster.getPartitions().getPartitions().get(localPartitionId), cluster);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return cluster.getEventloop();
	}

	// region JMX
	private boolean detailedStats;

	private final EventStats repartitionCount = EventStats.create(Duration.ofMinutes(5));
	private final PromiseStats repartitionPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final StreamStatsBasic<CrdtData<K, S>> repartitionStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> repartitionStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();
	// endregion

	public Promise<Void> repartition() {
		return repartition.get();
	}

	@NotNull
	private Promise<Void> doRepartition() {
		return Promises.toTuple(cluster.upload().toTry(), localClient.remove().toTry(), localClient.download().toTry())
				.then(all -> {
					if (!all.getValue1().isSuccess() || !all.getValue2().isSuccess() || !all.getValue3().isSuccess()) {
						CrdtException exception = new CrdtException("Repartition exceptions:");
						all.getValue1().consume(AsyncCloseable::close, exception::addSuppressed);
						all.getValue2().consume(AsyncCloseable::close, exception::addSuppressed);
						all.getValue3().consume(AsyncCloseable::close, exception::addSuppressed);
						return Promise.ofException(exception);
					}

					StreamConsumer<CrdtData<K, S>> cluster = all.getValue1().get()
							.transformWith(detailedStats ? repartitionStats : repartitionStatsDetailed);
					StreamConsumer<K> remover = all.getValue2().get()
							.transformWith(detailedStats ? removeStats : removeStatsDetailed);
					StreamSupplier<CrdtData<K, S>> downloader = all.getValue3().get();

					int index = this.cluster.getPartitions().indexOf(localPartitionId);
					assert index >= 0;

					StreamSplitter<CrdtData<K, S>, ?> splitter = StreamSplitter.create(
							(data, acceptors) -> {
								StreamDataAcceptor<Object> clusterAcceptor = acceptors[0];
								StreamDataAcceptor<Object> removeAcceptor = acceptors[1];
								clusterAcceptor.accept(data);
								int[] selected = this.cluster.getPartitions().shard(data.getKey());
								for (int s : selected) {
									if (s == index) {
										return;
									}
								}
								removeAcceptor.accept(data.getKey());
							});

					//noinspection unchecked, rawtypes
					((StreamSupplier) splitter.newOutput()).streamTo(cluster);
					//noinspection unchecked, rawtypes
					((StreamSupplier) splitter.newOutput()).streamTo(remover);

					return downloader.streamTo(splitter.getInput());
				});
	}

	// region JMX
	@JmxAttribute
	public boolean isDetailedStats() {
		return detailedStats;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public String getLocalPartitionId() {
		return localPartitionId.toString();
	}

	@JmxAttribute
	public EventStats getRepartitionCount() {
		return repartitionCount;
	}

	@JmxAttribute
	public PromiseStats getRepartitionPromise() {
		return repartitionPromise;
	}

	@JmxAttribute
	public StreamStatsBasic<CrdtData<K, S>> getRepartitionStats() {
		return repartitionStats;
	}

	@JmxAttribute
	public StreamStatsDetailed<CrdtData<K, S>> getRepartitionStatsDetailed() {
		return repartitionStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic<K> getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed<K> getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}
	// endregion
}
