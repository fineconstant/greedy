package org.kduda.greedy.service.exploration;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.kduda.greedy.algorithm.DecisionTableFactory;
import org.kduda.greedy.algorithm.m.HeuristicsM;
import org.kduda.greedy.algorithm.parser.ToStringParser;
import org.kduda.greedy.domain.FileContentTypes;
import org.kduda.greedy.model.ExploreRequestModel;
import org.kduda.greedy.model.FileModel;
import org.kduda.greedy.repository.data.FileRepository;
import org.kduda.greedy.repository.rules.RulesRepository;
import org.kduda.greedy.spark.reader.mongo.SparkMongoService;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Service
public class DataExplorationService implements ExplorationService {

	private final SparkMongoService sparkMongo;
	private final RulesRepository rulesRepository;
	private final FileRepository fileRepository;

	public DataExplorationService(SparkMongoService sparkMongo, RulesRepository rulesRepository, FileRepository fileRepository) {
		this.sparkMongo = sparkMongo;
		this.rulesRepository = rulesRepository;
		this.fileRepository = fileRepository;
	}

	@Override
	public void explore(String id, ExploreRequestModel requestModel) {
		Dataset<Row> csv = sparkMongo.readCsvById(id);

		Dataset<Row>[] dt;

		if ("is".equals(requestModel.getType()))
			dt = DecisionTableFactory.extractDecisionTables(csv);
		else
			//noinspection unchecked
			dt = new Dataset[]{csv};

		Dataset<Row>[] dtConsistent = DecisionTableFactory.removeInconsistenciesMCD(dt);
		Map<String, Dataset<Row>> dtMapped = DecisionTableFactory.createMapOf(dtConsistent);
		Map<String, List<List<Tuple2<String, String>>>> rules = HeuristicsM.calculateDecisionRules(dtMapped);

		String result;

		if ("csv".equals(requestModel.getOutput()))
			result = ToStringParser.buildStringCSV(rules);
		else
			result = ToStringParser.buildStringRSES(rules);

		FileModel fileModel = fileRepository.listById(id);
		String fileName = fileModel.getName().split("\\.")[0];

		InputStream stream = IOUtils.toInputStream(result, StandardCharsets.UTF_8);
		rulesRepository.store(stream, fileName + "-rules-m.csv", id, FileContentTypes.CSV.getType());

	}
}
