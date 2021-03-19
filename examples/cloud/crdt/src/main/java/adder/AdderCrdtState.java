package adder;

import io.activej.crdt.primitives.CrdtMergable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class AdderCrdtState implements CrdtMergable<AdderCrdtState> {
	private final Map<String, Float> deltas;

	public AdderCrdtState() {
		this.deltas = new LinkedHashMap<>();
	}

	public AdderCrdtState(LinkedHashMap<String, Float> deltas) {
		this.deltas = new LinkedHashMap<>(deltas);
	}

	public static AdderCrdtState of(String eventId, float delta) {
		LinkedHashMap<String, Float> map = new LinkedHashMap<>();
		map.put(eventId, delta);
		return new AdderCrdtState(map);
	}

	public float value() {
		float sum = 0;
		for (Float value : deltas.values()) {
			sum += value;
		}
		return sum;
	}

	public Map<String, Float> getDeltas() {
		return new HashMap<>(deltas);
	}

	@Override
	public AdderCrdtState merge(AdderCrdtState other) {
		AdderCrdtState newState = new AdderCrdtState();
		newState.deltas.putAll(this.deltas);

		other.deltas.forEach((eventId, delta) ->
				newState.deltas.merge(eventId, delta, (delta1, delta2) -> {
					if (!delta1.equals(delta2)) {
						throw new IllegalStateException("Deltas for the same event ID differ");
					}
					return delta1;
				}));

		return newState;
	}
}
