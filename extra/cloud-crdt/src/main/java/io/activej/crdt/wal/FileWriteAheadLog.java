package io.activej.crdt.wal;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.TruncatedDataException;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamSorter;
import io.activej.datastream.processor.StreamSorterStorageImpl;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.crdt.wal.FileWriteAheadLog.FlushMode.*;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Collections.singleton;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toList;

public class FileWriteAheadLog<K extends Comparable<K>, S> implements WriteAheadLog<K, S>, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(FileWriteAheadLog.class);

	public static final String EXT_FINAL = ".wal";
	public static final String EXT_CURRENT = ".current";
	public static final FrameFormat FRAME_FORMAT = LZ4FrameFormat.create();

	private static final int DEFAULT_SORT_ITEMS_IN_MEMORY = ApplicationSettings.getInt(FileWriteAheadLog.class, "sortItemsInMemory", 100_000);

	private final Eventloop eventloop;
	private final Executor executor;
	private final Path path;
	private final CrdtFunction<S> function;
	private final CrdtDataSerializer<K, S> serializer;
	private final CrdtStorage<K, S> storage;

	@Nullable
	private Path sortDir;
	private int sortItemsInMemory = DEFAULT_SORT_ITEMS_IN_MEMORY;

	private final Function<CrdtData<K, S>, K> keyFn = CrdtData::getKey;

	private final AsyncSupplier<Void> flush = AsyncSuppliers.coalesce(this::doFlush);

	private WalConsumer consumer;
	private boolean stopping;
	private boolean flushRequired;
	private boolean scanLostFiles = true;
	private FlushMode flushMode = UPLOAD_TO_STORAGE;

	private FileWriteAheadLog(
			Eventloop eventloop,
			Executor executor,
			Path path,
			CrdtFunction<S> function,
			CrdtDataSerializer<K, S> serializer,
			CrdtStorage<K, S> storage
	) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.path = path;
		this.function = function;
		this.serializer = serializer;
		this.storage = storage;
	}

	public static <K extends Comparable<K>, S> FileWriteAheadLog<K, S> create(
			Eventloop eventloop,
			Executor executor,
			Path path,
			CrdtFunction<S> function,
			CrdtDataSerializer<K, S> serializer,
			CrdtStorage<K, S> storage
	) {
		return new FileWriteAheadLog<>(eventloop, executor, path, function, serializer, storage);
	}

	public FileWriteAheadLog<K, S> withSortDir(Path sortDir) {
		this.sortDir = sortDir;
		return this;
	}

	public FileWriteAheadLog<K, S> withSortItemsInMemory(int sortItemsInMemory) {
		this.sortItemsInMemory = sortItemsInMemory;
		return this;
	}

	public FileWriteAheadLog<K, S> withFlushMode(FlushMode flushMode) {
		this.flushMode = flushMode;
		return this;
	}

	@Override
	public Promise<Void> put(K key, S value) {
		logger.trace("Putting value {} at key {}", value, key);
		flushRequired = true;
		return consumer.accept(new CrdtData<>(key, value));
	}

	@Override
	public Promise<Void> flush() {
		logger.trace("Flush called");
		return flush.get();
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		return scanLostFiles()
				.then(this::flushFiles)
				.whenResult(() -> this.consumer = createConsumer());
	}

	@Override
	public @NotNull Promise<?> stop() {
		stopping = true;
		if (flushRequired) return flush();

		return deleteWalFiles(singleton(consumer.walFile));
	}

	@Nullable
	private WalConsumer createConsumer() {
		return stopping ? null : new WalConsumer(path.resolve(UUID.randomUUID() + EXT_CURRENT));
	}

	private Promise<Void> doFlush() {
		if (!flushRequired) {
			logger.trace("Nothing to flush");
			return Promise.complete();
		}
		flushRequired = false;

		logger.trace("Begin flushing write ahead log");

		WalConsumer finishedConsumer = consumer;
		consumer = createConsumer();

		return finishedConsumer.finish()
				.then(() -> Promise.ofBlockingRunnable(executor, () -> rename(finishedConsumer.walFile))
						.whenException(e -> scanLostFiles = true))
				.then(this::scanLostFiles)
				.then(this::flushFiles)
				.whenException(e -> flushRequired = true)
				.whenComplete(toLogger(logger, TRACE, TRACE, "doFlush", this));
	}

	private Promise<Void> flushFiles() {
		if (flushMode == ROTATE_FILE) {
			return Promise.complete();
		} else if (flushMode == ROTATE_FILE_AWAIT) {
			return awaitExternalFlush();
		} else {
			assert flushMode == UPLOAD_TO_STORAGE;
			return getWalFiles()
					.then(this::flushWaLFiles);
		}
	}

	private Promise<Void> scanLostFiles() {
		if (!scanLostFiles) return Promise.complete();

		return getLostFiles()
				.then(lostFiles ->
						Promise.ofBlockingRunnable(executor, () -> {
							for (Path lostFile : lostFiles) {
								rename(lostFile);
							}
						}))
				.whenResult(() -> scanLostFiles = false);
	}

	private void rename(Path from) throws IOException {
		assert from.toString().endsWith(EXT_CURRENT);
		assert consumer == null || !from.equals(consumer.getWalFile());

		String filename = from.getFileName().toString();
		Path to = from.resolveSibling(filename.replace(EXT_CURRENT, EXT_FINAL));
		try {
			Files.move(from, to, ATOMIC_MOVE);
		} catch (AtomicMoveNotSupportedException ignored) {
			Files.move(from, to);
		}
	}

	private Promise<Void> awaitExternalFlush() {
		return getWalFiles()
				.then(walFiles -> {
					if (walFiles.isEmpty()) return Promise.complete();
					return Promises.delay(Duration.ofSeconds(1))
							.then(this::awaitExternalFlush);
				});
	}

	private Promise<Void> flushWaLFiles(List<Path> walFiles) {
		if (walFiles.isEmpty()) return Promise.complete();

		logger.info("Flushing write ahead logs {}", walFiles);

		Path sortDir;
		try {
			sortDir = createSortDir();
		} catch (IOException e) {
			logger.warn("Failed to create temporary sort directory", e);
			return Promise.ofException(e);
		}

		return createReducer(walFiles)
				.getOutput()
				.transformWith(createSorter(sortDir))
				.streamTo(storage.upload())
				.whenComplete(() -> cleanup(sortDir))
				.then(() -> deleteWalFiles(walFiles));
	}

	private Promise<List<Path>> getWalFiles() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Stream<Path> list = Files.list(path)) {
						return list
								.filter(file -> Files.isRegularFile(file) && file.toString().endsWith(EXT_FINAL))
								.collect(toList());
					}
				})
				.whenResult(walFiles -> logger.trace("Found {} write ahead logs {}", walFiles.size(), walFiles));
	}

	private Promise<List<Path>> getLostFiles() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Stream<Path> list = Files.list(path)) {
						return list
								.filter(file -> Files.isRegularFile(file) &&
										file.toString().endsWith(EXT_CURRENT) &&
										(consumer == null || !file.equals(consumer.getWalFile())))
								.collect(toList());
					}
				})
				.whenResult(walFiles -> logger.trace("Found {} lost files {}", walFiles.size(), walFiles));
	}

	private Promise<Void> deleteWalFiles(Collection<Path> walFiles) {
		logger.trace("Deleting write ahead logs: {}", walFiles);
		return Promise.ofBlockingRunnable(executor, () -> {
			for (Path walFile : walFiles) {
				Files.deleteIfExists(walFile);
			}
		});
	}

	private StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> createReducer(List<Path> files) {
		StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> reducer = StreamReducer.create();

		for (Path file : files) {
			ChannelSupplier.ofPromise(ChannelFileReader.open(executor, file))
					.transformWith(ChannelFrameDecoder.create(FRAME_FORMAT))
					.withEndOfStream(eos -> eos
							.thenEx(($, e) -> {
								if (e == null) return Promise.complete();
								if (e instanceof TruncatedDataException) {
									logger.warn("Write ahead log {} was truncated", file);
									return Promise.complete();
								}
								return Promise.ofException(e);
							}))
					.transformWith(ChannelDeserializer.create(serializer))
					.streamTo(reducer.newInput(CrdtData::getKey, new WalReducer()));
		}

		return reducer;
	}

	private StreamSorter<K, CrdtData<K, S>> createSorter(Path sortDir) {
		StreamSorterStorageImpl<CrdtData<K, S>> sorterStorage = StreamSorterStorageImpl.create(executor, serializer, FRAME_FORMAT, sortDir);
		return StreamSorter.create(sorterStorage, keyFn, naturalOrder(), false, sortItemsInMemory);
	}

	private Path createSortDir() throws IOException {
		if (this.sortDir != null) {
			return this.sortDir;
		} else {
			return Files.createTempDirectory("crdt-wal-sort-dir");
		}
	}

	private void cleanup(Path sortDir) {
		if (this.sortDir == null) {
			try {
				Files.deleteIfExists(sortDir);
			} catch (IOException ignored) {
			}
		}
	}

	private final class WalReducer implements StreamReducers.Reducer<K, CrdtData<K, S>, CrdtData<K, S>, CrdtData<K, S>> {

		@Override
		public CrdtData<K, S> onFirstItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> firstValue) {
			return firstValue;
		}

		@Override
		public CrdtData<K, S> onNextItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> nextValue, CrdtData<K, S> accumulator) {
			return new CrdtData<>(key, function.merge(accumulator.getState(), nextValue.getState()));
		}

		@Override
		public void onComplete(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> accumulator) {
			stream.accept(accumulator);
		}
	}

	private final class WalConsumer {
		private final AbstractStreamSupplier<CrdtData<K, S>> internalSupplier = new AbstractStreamSupplier<CrdtData<K, S>>() {
			@Override
			protected void onStarted() {
				resume();
			}
		};
		private final Path walFile;

		private SettablePromise<Void> writeCallback;

		public WalConsumer(Path walFile) {
			this.walFile = walFile;
			ChannelConsumer<ByteBuf> writer = ChannelConsumer.ofPromise(ChannelFileWriter.open(executor, walFile));
			internalSupplier.streamTo(StreamConsumer.ofSupplier(supplier -> supplier
					.transformWith(ChannelSerializer.create(serializer)
							.withAutoFlushInterval(Duration.ZERO))
					.transformWith(ChannelFrameEncoder.create(FRAME_FORMAT))
					.streamTo(ChannelConsumer.of(value -> {
						if (this.writeCallback == null) return writer.accept(value);

						SettablePromise<Void> writeCallback = this.writeCallback;
						this.writeCallback = null;
						return writer.accept(value)
								.whenComplete(writeCallback);
					}))));
		}

		public Path getWalFile() {
			return walFile;
		}

		public Promise<Void> accept(CrdtData<K, S> data) {
			if (this.writeCallback == null) {
				this.writeCallback = new SettablePromise<>();
			}
			SettablePromise<Void> writeCallback = this.writeCallback;
			internalSupplier.send(data);
			return writeCallback;
		}

		public Promise<Void> finish() {
			internalSupplier.sendEndOfStream();
			return internalSupplier.getAcknowledgement();
		}
	}

	public enum FlushMode {
		UPLOAD_TO_STORAGE,
		ROTATE_FILE,
		ROTATE_FILE_AWAIT
	}
}
