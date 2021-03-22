package adder;

import io.activej.common.Checks;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import static io.activej.common.Checks.checkArgument;

public class AdderCommands {
	public static final boolean CHECK = Checks.isEnabled(AdderCommands.class);

	public static final class PutRequest {
		private final long userId;
		private final float delta;

		public PutRequest(
				@Deserialize("userId") long userId,
				@Deserialize("delta") float delta) {
			if (CHECK) checkArgument(delta > 0);

			this.userId = userId;
			this.delta = delta;
		}

		@Serialize(order = 1)
		public long getUserId() {
			return userId;
		}

		@Serialize(order = 2)
		public float getDelta() {
			return delta;
		}
	}

	public enum PutResponse {
		INSTANCE
	}

	public static final class GetRequest {
		private final long userId;

		public GetRequest(@Deserialize("userId") long userId) {
			this.userId = userId;
		}

		@Serialize(order = 1)
		public long getUserId() {
			return userId;
		}
	}

	public static final class GetResponse {
		private final float sum;

		public GetResponse(@Deserialize("sum") float sum) {
			this.sum = sum;
		}

		@Serialize(order = 1)
		public float getSum() {
			return sum;
		}
	}
}
