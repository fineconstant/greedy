package org.kduda.greedy.service.exploration;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.kduda.greedy.algorithm.DecisionTableFactory;
import org.kduda.greedy.algorithm.log.HeuristicsLog;
import org.kduda.greedy.algorithm.m.HeuristicsM;
import org.kduda.greedy.algorithm.maxcov.HeuristicsMaxCov;
import org.kduda.greedy.algorithm.parser.ToStringParser;
import org.kduda.greedy.algorithm.poly.HeuristicsPoly;
import org.kduda.greedy.algorithm.rm.HeuristicsRM;
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

	private String fileFormat;
	private String heuristics;
	private String contentType;

	public DataExplorationService(SparkMongoService sparkMongo, RulesRepository rulesRepository, FileRepository fileRepository) {
		this.sparkMongo = sparkMongo;
		this.rulesRepository = rulesRepository;
		this.fileRepository = fileRepository;
	}

	@Override
	public void exploreAndSave(String id, ExploreRequestModel exploreDetails) {
		String fileName = getFileName(id);
		Dataset<Row> csv = sparkMongo.readCsvById(id);

		Dataset<Row>[] dts = prepareData(csv, exploreDetails);

		Dataset<Row>[] dtsConsistent = DecisionTableFactory.removeInconsistenciesMCD(dts);
		Map<String, Dataset<Row>> dtsMapped = DecisionTableFactory.createMapOf(dtsConsistent);

		Map<String, List<List<Tuple2<String, String>>>> rules = exploreWithHeuristics(dtsMapped, exploreDetails);

		String rulesString = buildStringOutput(rules, exploreDetails);

		persist(rulesString, fileName, exploreDetails, id);
	}

	private String getFileName(String id) {
		FileModel fileModel = fileRepository.listById(id);
		return fileModel.getName().split("\\.")[0];
	}

	private Dataset<Row>[] prepareData(Dataset<Row> csv, ExploreRequestModel exploreDetails) {
		Dataset<Row>[] dt;

		if ("is".equals(exploreDetails.getType()))
			dt = DecisionTableFactory.extractDecisionTables(csv);
		else
			//noinspection unchecked
			dt = new Dataset[]{csv};

		return dt;
	}

	private Map<String, List<List<Tuple2<String, String>>>> exploreWithHeuristics(Map<String, Dataset<Row>> dtsMapped,
																				  ExploreRequestModel exploreDetails) {
		switch (exploreDetails.getHeuristics()) {
			case "m":
				heuristics = "m";
				return HeuristicsM.calculateDecisionRules(dtsMapped);
			case "rm":
				heuristics = "rm";
				return HeuristicsRM.calculateDecisionRules(dtsMapped);
			case "maxCov":
				heuristics = "maxCov";
				return HeuristicsMaxCov.calculateDecisionRules(dtsMapped);
			case "poly":
				heuristics = "poly";
				return HeuristicsPoly.calculateDecisionRules(dtsMapped);
			case "log":
				heuristics = "log";
				return HeuristicsLog.calculateDecisionRules(dtsMapped);
			default:
				heuristics = "m";
				return HeuristicsM.calculateDecisionRules(dtsMapped);
		}
	}

	private String buildStringOutput(Map<String, List<List<Tuple2<String, String>>>> rules, ExploreRequestModel exploreDetails) {
		switch (exploreDetails.getOutput()) {
			case "csv":
				fileFormat = ".csv";
				contentType = FileContentTypes.CSV.getType();
				return ToStringParser.buildStringCSV(rules);
			case "rses":
				fileFormat = ".rul";
				contentType = FileContentTypes.RSES.getType();
				return ToStringParser.buildStringRSES(rules);
			default:
				fileFormat = ".csv";
				contentType = FileContentTypes.CSV.getType();
				return ToStringParser.buildStringCSV(rules);
		}
	}

	private void persist(String rulesString, String fileName, ExploreRequestModel exploreDetails, String id) {
		String dataType = exploreDetails.getType();
		String finalName = fileName + "-" + dataType + "-rules-" + heuristics + fileFormat;
		InputStream stream = IOUtils.toInputStream(rulesString, StandardCharsets.UTF_8);

		rulesRepository.store(stream, finalName, id, contentType);
	}
}
