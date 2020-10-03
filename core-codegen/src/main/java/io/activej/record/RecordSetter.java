package io.activej.record;

public interface RecordSetter {
	void set(Record record, Object value);

	default void setInt(Record record, int value) {
		throw new UnsupportedOperationException();
	}

	default void setLong(Record record, long value) {
		throw new UnsupportedOperationException();
	}

	default void setFloat(Record record, float value) {
		throw new UnsupportedOperationException();
	}

	default void setDouble(Record record, double value) {
		throw new UnsupportedOperationException();
	}

	RecordScheme getScheme();

	String getField();

	Class<?> getType();
}
