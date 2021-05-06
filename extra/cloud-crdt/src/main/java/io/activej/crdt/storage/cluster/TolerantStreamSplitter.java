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

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.util.RendezvousHashSharder;
import io.activej.datastream.*;
import io.activej.datastream.dsl.HasStreamInput;
import io.activej.datastream.dsl.HasStreamOutputs;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.activej.common.Checks.checkState;

@SuppressWarnings("unchecked")
final class TolerantStreamSplitter<K extends Comparable<K>, S> implements HasStreamInput<CrdtData<K, S>>, HasStreamOutputs<CrdtData<K, S>> {
	private final RendezvousHashSharder sharder;
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataAcceptor<CrdtData<K, S>>[] dataAcceptors = new StreamDataAcceptor[8];

	private int failed;
	private boolean started;

	TolerantStreamSplitter(RendezvousHashSharder sharder) {
		this.sharder = sharder;
		this.input = new Input();
	}

	public StreamSupplier<CrdtData<K, S>> newOutput() {
		checkState(!started, "Cannot add new inputs after StreamUnion has been started");
		Output output = new Output(outputs.size());
		outputs.add(output);
		if (outputs.size() > dataAcceptors.length) {
			dataAcceptors = Arrays.copyOf(dataAcceptors, dataAcceptors.length * 2);
		}
		return output;
	}

	public int getFailed() {
		return failed;
	}

	@Override
	public StreamConsumer<CrdtData<K, S>> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamSupplier<CrdtData<K, S>>> getOutputs() {
		return outputs;
	}

	void start() {
		started = true;
		dataAcceptors = Arrays.copyOf(dataAcceptors, outputs.size());
		input.getAcknowledgement()
				.whenException(e -> outputs.forEach(output -> output.closeEx(e)));
		Promises.all(outputs.stream().map(output -> output.getAcknowledgement()
				.thenEx(($, e) -> {
					if (e == null) return Promise.complete();

					if (++failed == dataAcceptors.length) {
						CrdtException exception = new CrdtException("All the partitions has disconnected");
						input.closeEx(exception);
						return Promise.ofException(exception);
					}
					sync();
					return Promise.complete();
				})))
				.whenResult(input::acknowledge);
		sync();
	}

	private final class Input extends AbstractStreamConsumer<CrdtData<K, S>> {
		@Override
		protected void onStarted() {
			sync();
		}

		@Override
		protected void onEndOfStream() {
			for (Output output : outputs) {
				output.sendEndOfStream();
			}
		}
	}

	private final class Output extends AbstractStreamSupplier<CrdtData<K, S>> {
		final int index;

		public Output(int index) {
			this.index = index;
		}

		@Override
		protected void onResumed() {
			dataAcceptors[index] = getDataAcceptor();
			sync();
		}

		@Override
		protected void onSuspended() {
			dataAcceptors[index] = getDataAcceptor();
			sync();
		}
	}

	private void sync() {
		if (!started) return;
		if (outputs.stream().allMatch(output -> output.isReady() || output.isComplete())) {
			input.resume(item -> {
				for (int index : sharder.shard(item.getKey())) {
					dataAcceptors[index].accept(item);
				}
			});
		} else {
			input.suspend();
		}
	}

}
