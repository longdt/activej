package adder;

import io.activej.common.api.Initializer;
import io.activej.common.collection.CollectionUtils;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.hash.CrdtMap;
import io.activej.crdt.hash.JavaCrdtMap;
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
	@SuppressWarnings("ConstantConditions")
	Map<Class<?>, RpcRequestHandler<?, ?>> handlers(
			CrdtMap<Long, AdderCrdtState> map,
			WriteAheadLog<Long, AdderCrdtState> writeAheadLog
	) {
		return CollectionUtils.map(
				PutRequest.class, (RpcRequestHandler<PutRequest, PutResponse>) request -> {
					long userId = request.getUserId();
					AdderCrdtState adderState = AdderCrdtState.of(request.getEventId(), request.getDelta());
					return writeAheadLog.put(userId, adderState)
							.then(() -> map.put(userId, adderState))
							.map($ -> PutResponse.INSTANCE);
				},
				GetRequest.class, (RpcRequestHandler<GetRequest, GetResponse>) request ->
						map.get(request.getUserId())
								.map(AdderCrdtState::value)
								.map(GetResponse::new)
		);
	}

	@Provides
	CrdtMap<Long, AdderCrdtState> map(Eventloop eventloop, CrdtFunction<AdderCrdtState> function, CrdtStorage<Long, AdderCrdtState> storage) {
		return new JavaCrdtMap<>(eventloop, function, storage);
	}

	@Provides
	CrdtFunction<AdderCrdtState> function() {
		return new CrdtFunction<AdderCrdtState>() {
			@Override
			public AdderCrdtState merge(AdderCrdtState first, AdderCrdtState second) {
				return first.merge(second);
			}

			@Override
			public AdderCrdtState extract(AdderCrdtState state, long timestamp) {
				return state;
			}
		};
	}

	@Provides
	WriteAheadLog<Long, AdderCrdtState> writeAheadLog(Eventloop eventloop, CrdtFunction<AdderCrdtState> function, CrdtStorage<Long, AdderCrdtState> storage) {
		return new InMemoryWriteAheadLog<>(eventloop, function, storage);
	}

	@Provides
	CrdtStorage<Long, AdderCrdtState> storage(Eventloop eventloop, CrdtFunction<AdderCrdtState> function) {
		return CrdtStorageMap.create(eventloop, function);
	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts CrdtMap only after it has started the WriteAheadLog
		return settings -> settings.addDependency(new Key<CrdtMap<Long, AdderCrdtState>>() {}, new Key<WriteAheadLog<Long, AdderCrdtState>>() {});
	}
}
