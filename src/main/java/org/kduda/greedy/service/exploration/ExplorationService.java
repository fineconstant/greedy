package org.kduda.greedy.service.exploration;

import org.kduda.greedy.model.ExploreRequestModel;

public interface ExplorationService {
	void explore(String id, ExploreRequestModel requestModel);
}
