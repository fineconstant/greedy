package org.kduda.greedy.domain;

public enum FileContentTypes {
	CSV("text/csv");

	private final String type;

	FileContentTypes(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
