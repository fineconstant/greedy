package org.kduda.greedy.model;

public class ExploreRequestModel {
	private String heuristics;
	private String type;

	public String getHeuristics() {
		return heuristics;
	}

	public void setHeuristics(String heuristics) {
		this.heuristics = heuristics;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "ExploreRequestModel{" +
			"heuristics='" + heuristics + '\'' +
			", type='" + type + '\'' +
			'}';
	}
}
