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

package io.activej.crdt;

import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.serializer.BinarySerializer;

import java.net.InetAddress;

import static io.activej.crdt.CrdtMessaging.*;
import static io.activej.crdt.CrdtMessaging.CrdtResponses.DOWNLOAD_STARTED;
import static io.activej.json.CspJsonUtils.nullTerminatedJson;

public final class CrdtServer<K extends Comparable<K>, S> extends AbstractServer<CrdtServer<K, S>> {
	private final CrdtStorage<K, S> storage;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<K> keySerializer;

	private CrdtServer(Eventloop eventloop, CrdtStorage<K, S> storage, CrdtDataSerializer<K, S> serializer) {
		super(eventloop);
		this.storage = storage;
		this.serializer = serializer;

		keySerializer = serializer.getKeySerializer();
	}

	public static <K extends Comparable<K>, S> CrdtServer<K, S> create(Eventloop eventloop, CrdtStorage<K, S> storage, CrdtDataSerializer<K, S> serializer) {
		return new CrdtServer<>(eventloop, storage, serializer);
	}

	public static <K extends Comparable<K>, S> CrdtServer<K, S> create(Eventloop eventloop, CrdtStorage<K, S> storage, BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new CrdtServer<>(eventloop, storage, new CrdtDataSerializer<>(keySerializer, stateSerializer));
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<CrdtMessage, CrdtResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, nullTerminatedJson(CrdtMessage.class, CrdtResponse.class));
		messaging.receive()
				.then(msg -> {
					if (msg == CrdtMessages.UPLOAD) {
						return messaging.receiveBinaryStream()
								.transformWith(ChannelDeserializer.create(serializer))
								.streamTo(StreamConsumer.ofPromise(storage.upload()))
								.then(() -> messaging.send(CrdtResponses.UPLOAD_FINISHED))
								.then(messaging::sendEndOfStream)
								.whenResult(messaging::close);

					}
					if (msg == CrdtMessages.REMOVE) {
						return messaging.receiveBinaryStream()
								.transformWith(ChannelDeserializer.create(keySerializer))
								.streamTo(StreamConsumer.ofPromise(storage.remove()))
								.then(() -> messaging.send(CrdtResponses.REMOVE_FINISHED))
								.then(messaging::sendEndOfStream)
								.whenResult(messaging::close);
					}
					if (msg instanceof Download) {
						return storage.download(((Download) msg).getToken())
								.whenResult(() -> messaging.send(DOWNLOAD_STARTED))
								.then(supplier -> supplier
										.transformWith(ChannelSerializer.create(serializer))
										.streamTo(messaging.sendBinaryStream()));
					}
					throw new IllegalArgumentException("Message type was added, but no handling code for it");
				})
				.whenComplete(($, e) -> {
					if (e == null) {
						return;
					}
					logger.warn("got an error while handling message {}", this, e);
					messaging.send(new ServerError(e.getClass().getSimpleName() + ": " + e.getMessage()))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}
}
