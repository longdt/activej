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

package io.activej.fs.tcp;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FsException;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.activej.common.collection.CollectionUtils.toLimitedString;

public final class RemoteFsResponses {

	@CompiledJson(discriminator = "Type")
	public abstract static class FsResponse {
	}

	@CompiledJson(name = "UploadAck")
	public static final class UploadAck extends FsResponse {
		@Override
		public String toString() {
			return "UploadAck{}";
		}
	}

	@CompiledJson(name = "UploadFinished")
	public static final class UploadFinished extends FsResponse {
		@Override
		public String toString() {
			return "UploadFinished{}";
		}
	}

	@CompiledJson(name = "AppendAck")
	public static final class AppendAck extends FsResponse {
		@Override
		public String toString() {
			return "AppendAck{}";
		}
	}

	@CompiledJson(name = "AppendFinished")
	public static final class AppendFinished extends FsResponse {
		@Override
		public String toString() {
			return "AppendFinished{}";
		}
	}

	@CompiledJson(name = "DownloadSize")
	public static final class DownloadSize extends FsResponse {
		private final long size;

		public DownloadSize(long size) {
			this.size = size;
		}

		public long getSize() {
			return size;
		}

		@Override
		public String toString() {
			return "DownloadSize{size=" + size + '}';
		}
	}

	@CompiledJson(name = "CopyFinished")
	public static final class CopyFinished extends FsResponse {
		@Override
		public String toString() {
			return "CopyFinished{}";
		}
	}

	@CompiledJson(name = "CopyAllFinished")
	public static final class CopyAllFinished extends FsResponse {
		@Override
		public String toString() {
			return "CopyAllFinished{}";
		}
	}

	@CompiledJson(name = "MoveFinished")
	public static final class MoveFinished extends FsResponse {
		@Override
		public String toString() {
			return "MoveFinished{}";
		}
	}

	@CompiledJson(name = "MoveAllFinished")
	public static final class MoveAllFinished extends FsResponse {
		@Override
		public String toString() {
			return "MoveAllFinished{}";
		}
	}

	@CompiledJson(name = "ListFinished")
	public static final class ListFinished extends FsResponse {
		private final Map<String, FileMetadata> files;

		public ListFinished(Map<String, FileMetadata> files) {
			this.files = Collections.unmodifiableMap(files);
		}

		public Map<String, FileMetadata> getFiles() {
			return files;
		}

		@Override
		public String toString() {
			return "ListFinished{files=" + files.size() + '}';
		}
	}

	@CompiledJson(name = "DeleteFinished")
	public static final class DeleteFinished extends FsResponse {
		@Override
		public String toString() {
			return "DeleteFinished{}";
		}
	}

	@CompiledJson(name = "DeleteAllFinished")
	public static final class DeleteAllFinished extends FsResponse {
		@Override
		public String toString() {
			return "DeleteAllFinished{}";
		}
	}

	@CompiledJson(name = "ServerError")
	public static final class ServerError extends FsResponse {
		private final FsException error;

		public ServerError(FsException error) {
			this.error = error;
		}

		@JsonAttribute
		public FsException getError() {
			return error;
		}

		@Override
		public String toString() {
			return "ServerError{error=" + error + '}';
		}
	}

	@CompiledJson(name = "InfoFinished")
	public static final class InfoFinished extends FsResponse {
		@Nullable
		private final FileMetadata metadata;

		public InfoFinished(@Nullable FileMetadata metadata) {
			this.metadata = metadata;
		}

		@Nullable
		public FileMetadata getMetadata() {
			return metadata;
		}

		@Override
		public String toString() {
			return "InfoFinished{metadata=" + metadata + '}';
		}
	}

	@CompiledJson(name = "InfoAllFinished")
	public static final class InfoAllFinished extends FsResponse {
		private final Map<String, FileMetadata> metadataMap;

		public InfoAllFinished(Map<String, FileMetadata> metadataMap) {
			this.metadataMap = metadataMap;
		}

		public Map<String, FileMetadata> getMetadataMap() {
			return metadataMap;
		}

		@Override
		public String toString() {
			return "InfoAllFinished{metadataMap=" + toLimitedString(metadataMap, 50) + '}';
		}
	}

	@CompiledJson(name = "PingFinished")
	public static final class PingFinished extends FsResponse {
		@Override
		public String toString() {
			return "PingFinished{}";
		}
	}
}
