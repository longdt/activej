package io.activej.launchers.crdt.rpc;

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Optional;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategyList;
import io.activej.rpc.hash.ShardingFunction;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofList;
import static io.activej.launchers.crdt.rpc.CrdtRpcServerModule.DEFAULT_PORT;

public class CrdtRpcClientModule extends AbstractModule {
	public static final String PROPERTIES_FILE = "crdt-rpc-client.properties";

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("address", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(DEFAULT_PORT)))
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Provides
	RpcClient client(Eventloop eventloop, RpcStrategy strategy, List<Class<?>> messageTypes) {
		return RpcClient.create(eventloop)
				.withMessageTypes(messageTypes)
				.withStrategy(strategy);
	}

	@Provides
	RpcStrategy strategy(Config config, @Optional ShardingFunction<?> shardingFn) {
		List<InetSocketAddress> addresses = config.get(ofList(ofInetSocketAddress()), "addresses", Collections.emptyList());

		if (addresses.isEmpty()) {
			InetSocketAddress address = config.get(ofInetSocketAddress(), "address");
			return RpcStrategies.server(address);
		}

		return RpcStrategies.sharding(checkNotNull(shardingFn), RpcStrategyList.ofAddresses(addresses));
	}
}
