package org.kduda.greedy.domain;

public enum TableTypes {
	NONE(""),
	INFOTMATION_SYSTEM("information-system"),
	DECISION_TABLE("decision-table");

	private final String value;

	TableTypes(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
