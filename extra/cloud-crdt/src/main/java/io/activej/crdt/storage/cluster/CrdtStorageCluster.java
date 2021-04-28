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

import io.activej.async.service.EventloopService;
import io.activej.common.api.WithInitializer;
import io.activej.common.collection.Try;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.function.CrdtFilter;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers.BinaryAccumulatorReducer;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;
import static io.activej.crdt.util.Utils.wrapException;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("rawtypes") // JMX
public final class CrdtStorageCluster<K extends Comparable<K>, S> implements CrdtStorage<K, S>, WithInitializer<CrdtStorageCluster<K, S>>, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CrdtStorageCluster.class);

	private final CrdtPartitions<K, S> partitions;

	private final CrdtFunction<S> function;

	/**
	 * Maximum allowed number of dead partitions, if there are more dead partitions than this number,
	 * the cluster is considered malformed.
	 */
	private int deadPartitionsThreshold = 0;

	/**
	 * Number of uploads that are initiated.
	 */
	private int uploadTargets = 1;

	private CrdtFilter<S> filter;

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();
	// endregion

	// region creators
	private CrdtStorageCluster(CrdtPartitions<K, S> partitions, CrdtFunction<S> function) {
		this.partitions = partitions;
		this.function = function;
	}

	public static <K extends Comparable<K>, S> CrdtStorageCluster<K, S> create(
			CrdtPartitions<K, S> partitions, CrdtFunction<S> crdtFunction
	) {
		return new CrdtStorageCluster<>(partitions, crdtFunction);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> CrdtStorageCluster<K, S> create(
			CrdtPartitions<K, S> partitions
	) {
		return new CrdtStorageCluster<>(partitions, CrdtFunction.ofCrdtType());
	}

	public CrdtStorageCluster<K, S> withReplicationCount(int replicationCount) {
		checkArgument(1 <= replicationCount && replicationCount <= partitions.getPartitions().size(),
				"Replication count cannot be less than one or greater than number of partitions");
		this.deadPartitionsThreshold = replicationCount - 1;
		this.uploadTargets = replicationCount;
		this.partitions.setTopShards(replicationCount);
		return this;
	}

	public CrdtStorageCluster<K, S> withFilter(CrdtFilter<S> filter) {
		this.filter = filter;
		return this;
	}

	/**
	 * Sets the replication count as well as number of upload targets that determines the number of server where CRDT data will be uploaded.
	 */
	@SuppressWarnings("UnusedReturnValue")
	public CrdtStorageCluster<K, S> withPersistenceOptions(int deadPartitionsThreshold, int uploadTargets) {
		checkArgument(0 <= deadPartitionsThreshold && deadPartitionsThreshold < partitions.getPartitions().size(),
				"Dead partitions threshold cannot be less than zero or greater than number of partitions");
		checkArgument(0 <= uploadTargets,
				"Number of upload targets should not be less than zero");
		checkArgument(uploadTargets <= partitions.getPartitions().size(),
				"Number of upload targets should not exceed total number of partitions");
		this.deadPartitionsThreshold = deadPartitionsThreshold;
		this.uploadTargets = uploadTargets;
		return this;
	}
	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return partitions.getEventloop();
	}

	private <T> Promise<List<Container<T>>> connect(Function<CrdtStorage<K, S>, Promise<T>> method) {
		return Promises.toList(
				partitions.getAlivePartitions().entrySet().stream()
						.map(entry ->
								method.apply(entry.getValue())
										.map(t -> new Container<>(entry.getKey(), t))

										.whenException(err -> partitions.markDead(entry.getKey(), err))
										.toTry()))
				.then(this::checkStillNotDead)
				.then(tries -> {
					List<Container<T>> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());
					if (successes.isEmpty()) {
						return Promise.ofException(new CrdtException("No successful connections"));
					}
					return Promise.of(successes);
				});
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect(CrdtStorage::upload)
				.map(containers -> {
					StreamSplitter<CrdtData<K, S>, CrdtData<K, S>> splitter = StreamSplitter.create(
							(item, acceptors) -> {
								for (int index : partitions.shard(item.getKey())) {
									acceptors[index].accept(item);
								}
							});
					for (Container<StreamConsumer<CrdtData<K, S>>> container : containers) {
						splitter.newOutput().streamTo(container.value);
					}
					return splitter.getInput()
							.transformWith(detailedStats ? uploadStats : uploadStatsDetailed)
							.withAcknowledgement(ack -> ack
									.thenEx(wrapException(() -> "Cluster 'upload' failed")));
				});
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return connect(storage -> storage.download(timestamp))
				.map(containers -> {
					StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> streamReducer = StreamReducer.create();
					for (Container<StreamSupplier<CrdtData<K, S>>> container : containers) {
						container.value.streamTo(streamReducer.newInput(CrdtData::getKey, filter == null ?
								new BinaryAccumulatorReducer<>((a, b) -> new CrdtData<>(a.getKey(), function.merge(a.getState(), b.getState()))) :
								new BinaryAccumulatorReducer<K, CrdtData<K, S>>((a, b) -> new CrdtData<>(a.getKey(), function.merge(a.getState(), b.getState()))) {
									@Override
									protected boolean filter(CrdtData<K, S> value) {
										return filter.test(value.getState());
									}
								}));
					}
					return streamReducer.getOutput()
							.transformWith(detailedStats ? downloadStats : downloadStatsDetailed)
							.withEndOfStream(eos -> eos
									.thenEx(wrapException(() -> "Cluster 'download' failed")));
				});
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return connect(CrdtStorage::remove)
				.map(containers -> {
					StreamSplitter<K, K> splitter = StreamSplitter.create((item, acceptors) -> {
						for (StreamDataAcceptor<K> acceptor : acceptors) {
							acceptor.accept(item);
						}
					});
					containers.forEach(container -> splitter.newOutput().streamTo(container.value));
					return splitter.getInput()
							.transformWith(detailedStats ? removeStats : removeStatsDetailed)
							.withAcknowledgement(ack -> ack
									.thenEx(wrapException(() -> "Cluster 'remove' failed")));
				});
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete();  // Promises.all(aliveClients.values().stream().map(CrdtClient::ping));
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	private <T> Promise<T> checkStillNotDead(T value) {
		Map<Comparable<?>, CrdtStorage<K, S>> deadPartitions = partitions.getDeadPartitions();
		if (deadPartitions.size() > deadPartitionsThreshold) {
			return Promise.ofException(new CrdtException("There are more dead partitions than allowed(" +
					deadPartitions.size() + " dead, threshold is " + deadPartitionsThreshold + "), aborting"));
		}
		return Promise.of(value);
	}

	// region JMX
	@JmxAttribute
	public int getDeadPartitionsThreshold() {
		return deadPartitionsThreshold;
	}

	@JmxAttribute
	public int getUploadTargets() {
		return uploadTargets;
	}

	@JmxOperation
	public void setPersistenceOptions(int deadPartitionsThreshold, int uploadTargets) {
		withPersistenceOptions(deadPartitionsThreshold, uploadTargets);
	}

	@JmxAttribute
	public void setReplicationCount(int replicationCount) {
		withReplicationCount(replicationCount);
	}

	@JmxAttribute(name = "")
	public CrdtPartitions<K, S> getPartitions() {
		return partitions;
	}

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
	public StreamStatsBasic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}
	// endregion

	private static class Container<T> {
		final Object id;
		final T value;

		Container(Object id, T value) {
			this.id = id;
			this.value = value;
		}
	}
}
