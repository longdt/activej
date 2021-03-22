package adder;

import io.activej.async.process.AsyncExecutor;
import io.activej.async.process.AsyncExecutors;
import io.activej.common.api.Initializer;
import io.activej.common.collection.CollectionUtils;
import io.activej.config.Config;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.hash.CrdtMap;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.wal.InMemoryWriteAheadLog;
import io.activej.crdt.wal.WriteAheadLog;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.service.ServiceGraphModuleSettings;

import java.util.Map;

import static adder.AdderCommands.*;

public class AdderServerModule extends AbstractModule {

	@Provides
	@ServerId
	String serverId(Config config) {
		return config.get("serverId");
	}

	@Provides
	@SuppressWarnings("ConstantConditions")
	Map<Class<?>, RpcRequestHandler<?, ?>> handlers(
			@ServerId String serverId,
			CrdtMap<Long, SimpleSumsCrdtState> map,
			WriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog
	) {
		AsyncExecutor sequentialExecutor = AsyncExecutors.sequential();
		return CollectionUtils.map(
				PutRequest.class, (RpcRequestHandler<PutRequest, PutResponse>) request -> {
					long userId = request.getUserId();
					return sequentialExecutor.execute(() -> map.get(userId)
							.then(state -> {
								float newSum = request.getDelta() +
										(state == null ?
												0 :
												state.getLocalSum());

								return writeAheadLog.put(userId, DetailedSumsCrdtState.of(serverId, newSum))
										.then(() -> map.put(userId, SimpleSumsCrdtState.of(newSum)))
										.map($ -> PutResponse.INSTANCE);
							}));
				},
				GetRequest.class, (RpcRequestHandler<GetRequest, GetResponse>) request ->
						map.get(request.getUserId())
								.map(SimpleSumsCrdtState::value)
								.map(GetResponse::new)
		);
	}

	@Provides
	CrdtMap<Long, SimpleSumsCrdtState> map(Eventloop eventloop, @ServerId String serverId, CrdtStorage<Long, DetailedSumsCrdtState> storage) {
		return new AdderCrdtMap(eventloop, serverId, storage);
	}

	@Provides
	CrdtFunction<DetailedSumsCrdtState> function() {
		return new CrdtFunction<DetailedSumsCrdtState>() {
			@Override
			public DetailedSumsCrdtState merge(DetailedSumsCrdtState first, DetailedSumsCrdtState second) {
				return first.merge(second);
			}

			@Override
			public DetailedSumsCrdtState extract(DetailedSumsCrdtState state, long timestamp) {
				return state;
			}
		};
	}

	@Provides
	WriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Eventloop eventloop,
			CrdtFunction<DetailedSumsCrdtState> function,
			CrdtStorage<Long, DetailedSumsCrdtState> storage
	) {
		return new InMemoryWriteAheadLog<>(eventloop, function, storage);
	}

	@Provides
	CrdtStorage<Long, DetailedSumsCrdtState> storage(Eventloop eventloop, CrdtFunction<DetailedSumsCrdtState> function) {
		return CrdtStorageMap.create(eventloop, function);
	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts CrdtMap only after it has started the WriteAheadLog
		return settings -> settings.addDependency(new Key<CrdtMap<Long, SimpleSumsCrdtState>>() {}, new Key<WriteAheadLog<Long, DetailedSumsCrdtState>>() {});
	}
}
