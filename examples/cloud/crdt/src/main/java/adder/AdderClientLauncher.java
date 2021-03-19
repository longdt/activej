package adder;

import adder.AdderCommands.PutRequest;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.crdt.rpc.CrdtRpcClientLauncher;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.hash.ShardingFunction;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static adder.AdderCommands.GetRequest;
import static adder.AdderCommands.GetResponse;
import static adder.AdderServerLauncher.MESSAGE_TYPES;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofList;
import static java.util.stream.Collectors.toList;

public final class AdderClientLauncher extends CrdtRpcClientLauncher {
	public static final int USER_IDS_SIZE = 100;
	public static final int MAX_DELTA = 100;
	public static final int LIMIT = 1000;

	private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
	private static final List<Long> USER_IDS = Stream.generate(() -> RANDOM.nextLong(10 * USER_IDS_SIZE))
			.distinct()
			.limit(USER_IDS_SIZE)
			.collect(toList());

	private final Map<Long, Float> controlMap = new TreeMap<>();

	@Inject
	Eventloop eventloop;

	@Inject
	RpcClient client;

	@Override
	protected List<Class<?>> getMessageTypes() {
		return MESSAGE_TYPES;
	}

	@Provides
	ShardingFunction<?> shardingFunction(Config config) {
		List<InetSocketAddress> addresses = config.get(ofList(ofInetSocketAddress()), "addresses", Collections.emptyList());
		if (addresses.isEmpty()) {
			return $ -> {
				throw new IllegalStateException();
			};
		}

		int shardsCount = addresses.size();
		return item -> {
			if (item instanceof PutRequest) {
				return (int) (((PutRequest) item).getUserId() % shardsCount);
			}
			assert item instanceof GetRequest;
			return (int) (((GetRequest) item).getUserId() % shardsCount);
		};
	}

	@Override
	protected void run() throws Exception {
		uploadDeltas();

		fetchSum(USER_IDS.get(RANDOM.nextInt(USER_IDS.size())));
		fetchSum(USER_IDS.get(RANDOM.nextInt(USER_IDS.size())));
		fetchSum(USER_IDS.get(RANDOM.nextInt(USER_IDS.size())));
		fetchSum(USER_IDS.get(RANDOM.nextInt(USER_IDS.size())));
		fetchSum(USER_IDS.get(RANDOM.nextInt(USER_IDS.size())));
	}

	private void uploadDeltas() throws Exception {
		eventloop.submit(() ->
				Promises.until(0, i -> Promises.all(USER_IDS.stream()
								.map(userId -> {
									float delta = RANDOM.nextFloat() * MAX_DELTA;
									String eventId = UUID.randomUUID().toString();
									return client.sendRequest(new PutRequest(userId, eventId, delta))
											.whenResult(() -> controlMap.merge(userId, delta, Float::sum));
								}))
								.map($ -> i + 1),
						i -> i == LIMIT
				)).get();
		System.out.println("Deltas are updated\n");
	}

	private void fetchSum(long randomUserId) throws Exception {
		float fetchedSum = eventloop.submit(() ->
				client.<GetRequest, GetResponse>sendRequest(new GetRequest(randomUserId))
						.map(GetResponse::getSum)
		).get();

		System.out.println("Fetched sum for user ID [" + randomUserId + "]: " + fetchedSum);
		float localSum = controlMap.get(randomUserId);
		System.out.println("Are sums equal? : " + (localSum == fetchedSum) + '\n');
	}

	public static void main(String[] args) throws Exception {
		new AdderClientLauncher().launch(args);
	}
}
