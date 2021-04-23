package io.activej.crdt.wal;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.TruncatedDataException;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamSorter;
import io.activej.datastream.processor.StreamSorterStorageImpl;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.activej.async.function.AsyncSuppliers.coalesce;
import static io.activej.crdt.util.Utils.deleteWalFiles;
import static io.activej.crdt.util.Utils.getWalFiles;
import static io.activej.crdt.wal.FileWriteAheadLog.FRAME_FORMAT;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toList;

public final class WalUploader<K extends Comparable<K>, S> implements EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(WalUploader.class);

	private static final int DEFAULT_SORT_ITEMS_IN_MEMORY = ApplicationSettings.getInt(WalUploader.class, "sortItemsInMemory", 100_000);
	private static final Duration SMOOTHING_WINDOW = ApplicationSettings.getDuration(WalUploader.class, "smoothingWindow", Duration.ofMinutes(5));

	private final AsyncSupplier<Void> uploadToStorage = coalesce(this::doUploadToStorage);

	private final Eventloop eventloop;
	private final Executor executor;
	private final Path path;
	private final CrdtFunction<S> function;
	private final CrdtDataSerializer<K, S> serializer;
	private final CrdtStorage<K, S> storage;

	private final PromiseStats uploadPromise = PromiseStats.create(SMOOTHING_WINDOW);
	private final ValueStats totalFilesUploaded = ValueStats.create(SMOOTHING_WINDOW);
	private final ValueStats totalFilesUploadedSize = ValueStats.create(SMOOTHING_WINDOW).withUnit("bytes");
	private boolean detailedMonitoring;

	@Nullable
	private Path sortDir;
	private int sortItemsInMemory = DEFAULT_SORT_ITEMS_IN_MEMORY;

	private WalUploader(Eventloop eventloop, Executor executor, Path path, CrdtFunction<S> function, CrdtDataSerializer<K, S> serializer, CrdtStorage<K, S> storage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.path = path;
		this.function = function;
		this.serializer = serializer;
		this.storage = storage;
	}

	public static <K extends Comparable<K>, S> WalUploader<K, S> create(Eventloop eventloop, Executor executor, Path path, CrdtFunction<S> function, CrdtDataSerializer<K, S> serializer, CrdtStorage<K, S> storage) {
		return new WalUploader<>(eventloop, executor, path, function, serializer, storage);
	}

	public WalUploader<K, S> withSortDir(Path sortDir) {
		this.sortDir = sortDir;
		return this;
	}

	public WalUploader<K, S> withSortItemsInMemory(int sortItemsInMemory) {
		this.sortItemsInMemory = sortItemsInMemory;
		return this;
	}

	public Promise<Void> uploadToStorage() {
		return uploadToStorage.get()
				.whenComplete(uploadPromise.recordStats());
	}

	private Promise<Void> doUploadToStorage() {
		return getWalFiles(executor, path)
				.then(this::uploadWaLFiles);
	}

	private Promise<Void> uploadWaLFiles(List<Path> walFiles) {
		if (walFiles.isEmpty()) {
			logger.info("Nothing to upload");
			return Promise.complete();
		}

		if (logger.isInfoEnabled()) {
			logger.info("Uploading write ahead logs {}", walFiles.stream().map(Path::getFileName).collect(toList()));
		}

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
				.whenResult(() -> {
					totalFilesUploaded.recordValue(walFiles.size());
					if (detailedMonitoring) {
						for (Path walFile : walFiles) {
							try {
								totalFilesUploadedSize.recordValue(Files.size(walFile));
							} catch (IOException e) {
								logger.warn("Could not get the size of uploaded file {}", walFile);
							}
						}
					}
				})
				.then(() -> deleteWalFiles(executor, walFiles));
	}

	private Path createSortDir() throws IOException {
		if (this.sortDir != null) {
			return this.sortDir;
		} else {
			return Files.createTempDirectory("crdt-wal-sort-dir");
		}
	}

	private StreamSorter<K, CrdtData<K, S>> createSorter(Path sortDir) {
		StreamSorterStorageImpl<CrdtData<K, S>> sorterStorage = StreamSorterStorageImpl.create(executor, serializer, FRAME_FORMAT, sortDir);
		Function<CrdtData<K, S>, K> keyFn = CrdtData::getKey;
		return StreamSorter.create(sorterStorage, keyFn, naturalOrder(), false, sortItemsInMemory);
	}

	private void cleanup(Path sortDir) {
		if (this.sortDir == null) {
			try {
				Files.deleteIfExists(sortDir);
			} catch (IOException ignored) {
			}
		}
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

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	// region JMX
	@JmxAttribute
	public PromiseStats getUploadPromise() {
		return uploadPromise;
	}

	@JmxAttribute
	public ValueStats getTotalFilesUploaded() {
		return totalFilesUploaded;
	}

	@JmxAttribute
	public ValueStats getTotalFilesUploadedSize() {
		return totalFilesUploadedSize;
	}

	@JmxAttribute
	public boolean isDetailedMonitoring() {
		return detailedMonitoring;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailedMonitoring = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedMonitoring = false;
	}
	// endregion

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
}
