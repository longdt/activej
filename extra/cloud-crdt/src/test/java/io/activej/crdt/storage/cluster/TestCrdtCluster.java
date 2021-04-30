package io.activej.crdt.storage.cluster;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.net.AbstractServer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.BinarySerializers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public final class TestCrdtCluster {
	private static final BinarySerializer<Set<Integer>> INT_SET_SERIALIZER = BinarySerializers.ofSet(INT_SERIALIZER);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testUpload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		CrdtDataSerializer<String, TimestampContainer<Integer>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER));

		List<CrdtServer<String, TimestampContainer<Integer>>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, TimestampContainer<Integer>>> clients = new HashMap<>();
		Map<String, CrdtStorageMap<String, TimestampContainer<Integer>>> remoteStorages = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			CrdtStorageMap<String, TimestampContainer<Integer>> storage = CrdtStorageMap.create(eventloop, TimestampContainer.createCrdtFunction(Integer::max));
			InetSocketAddress address = new InetSocketAddress(getFreePort());
			CrdtServer<String, TimestampContainer<Integer>> server = CrdtServer.create(eventloop, storage, serializer);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(eventloop, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}

		List<CrdtData<String, TimestampContainer<Integer>>> data = new ArrayList<>();
		for (int i = 0; i < 25; i++) {
			data.add(new CrdtData<>((char) (i + 97) + "", TimestampContainer.now(i + 1)));
		}
		CrdtStorageMap<String, TimestampContainer<Integer>> localStorage = CrdtStorageMap.create(eventloop, TimestampContainer.createCrdtFunction(Integer::max));
		for (CrdtData<String, TimestampContainer<Integer>> datum : data) {
			localStorage.put(datum);
		}
		CrdtPartitions<String, TimestampContainer<Integer>> partitions = CrdtPartitions.create(eventloop, clients);
		int replicationCount = 4;
		CrdtStorageCluster<String, TimestampContainer<Integer>> cluster = CrdtStorageCluster.create(partitions, TimestampContainer.createCrdtFunction(Integer::max))
				.withReplicationCount(replicationCount);

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(cluster.upload()))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		Map<CrdtData<String, TimestampContainer<Integer>>, Integer> result = new HashMap<>();

		remoteStorages.values().forEach(v -> v.iterator()
				.forEachRemaining(x -> result.compute(x, ($, count) -> count == null ? 1 : (count + 1))));

		assertEquals(new HashSet<>(data), result.keySet());
		for (Integer count : result.values()) {
			assertEquals(replicationCount, count.intValue());
		}
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testDownload() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		List<CrdtServer<String, TimestampContainer<Set<Integer>>>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, TimestampContainer<Set<Integer>>>> clients = new HashMap<>();
		Map<String, CrdtStorageMap<String, TimestampContainer<Set<Integer>>>> remoteStorages = new LinkedHashMap<>();

		CrdtFunction<TimestampContainer<Set<Integer>>> union = TimestampContainer.createCrdtFunction((a, b) -> {
			a.addAll(b);
			return a;
		});
		CrdtDataSerializer<String, TimestampContainer<Set<Integer>>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SET_SERIALIZER));

		String key1 = "test_1";
		String key2 = "test_2";
		String key3 = "test_3";
		for (int i = 0; i < 10; i++) {
			CrdtStorageMap<String, TimestampContainer<Set<Integer>>> storage = CrdtStorageMap.create(eventloop, union);

			storage.put(key1, TimestampContainer.now(new HashSet<>(singleton(i))));
			storage.put(key2, TimestampContainer.now(new HashSet<>(singleton(i / 2))));
			storage.put(key3, TimestampContainer.now(new HashSet<>(singleton(123))));

			InetSocketAddress address = new InetSocketAddress(getFreePort());
			CrdtServer<String, TimestampContainer<Set<Integer>>> server = CrdtServer.create(eventloop, storage, serializer);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(eventloop, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}

		CrdtStorageMap<String, TimestampContainer<Set<Integer>>> localStorage = CrdtStorageMap.create(eventloop, union);
		CrdtPartitions<String, TimestampContainer<Set<Integer>>> partitions = CrdtPartitions.create(eventloop, clients);
		CrdtStorageCluster<String, TimestampContainer<Set<Integer>>> cluster = CrdtStorageCluster.create(partitions, union);

		await(cluster.download()
				.then(supplier -> supplier
						.streamTo(StreamConsumer.of(localStorage::put))
						.whenComplete(() -> {
							System.out.println("!finish");
							servers.forEach(AbstractServer::close);
						})));

		assertEquals(set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), localStorage.get(key1).getState());
		assertEquals(set(0, 1, 2, 3, 4), localStorage.get(key2).getState());
		assertEquals(set(123), localStorage.get(key3).getState());
	}
}
