package adder;

import io.activej.crdt.primitives.CrdtMergable;

import java.util.HashMap;
import java.util.Map;

public final class DetailedSumsCrdtState implements CrdtMergable<DetailedSumsCrdtState> {
	private final Map<String, Float> sums;

	public DetailedSumsCrdtState() {
		this.sums = new HashMap<>();
	}

	public DetailedSumsCrdtState(Map<String, Float> sums) {
		this.sums = new HashMap<>(sums);
	}

	public static DetailedSumsCrdtState of(String serverId, float sum) {
		Map<String, Float> map = new HashMap<>();
		map.put(serverId, sum);
		return new DetailedSumsCrdtState(map);
	}

	public float getSumFor(String serverId) {
		return sums.get(serverId);
	}

	public float getSumExcept(String serverId) {
		float sum = 0;
		for (Map.Entry<String, Float> entry : sums.entrySet()) {
			if (entry.getKey().equals(serverId)) continue;
			sum += entry.getValue();
		}
		return sum;
	}

	@Override
	public DetailedSumsCrdtState merge(DetailedSumsCrdtState other) {
		DetailedSumsCrdtState newState = new DetailedSumsCrdtState();
		newState.sums.putAll(this.sums);

		other.sums.forEach((serverId, delta) -> newState.sums.merge(serverId, delta, Math::max));

		return newState;
	}
}
