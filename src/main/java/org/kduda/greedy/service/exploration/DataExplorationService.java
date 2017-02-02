package org.kduda.greedy.service.exploration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.kduda.greedy.model.ExploreRequestModel;
import org.kduda.greedy.spark.reader.mongo.SparkMongoService;
import org.springframework.stereotype.Service;

@Service
public class DataExplorationService implements ExplorationService {

	private final SparkMongoService sparkMongo;

	public DataExplorationService(SparkMongoService sparkMongo) {
		this.sparkMongo = sparkMongo;
	}

	@Override
	public void explore(String id, ExploreRequestModel requestModel) {
		Dataset<Row> csv = sparkMongo.readCsvById(id);

		// csv.show();
	}
}
