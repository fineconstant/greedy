package org.kduda.greedy.unit.algorithm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kduda.greedy.algorithm.DecisionTableFactory;
import org.kduda.greedy.algorithm.HeuristicsM;
import org.kduda.greedy.spark.reader.csv.SparkCsvReader;
import org.kduda.greedy.unit.SpringUnitTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HeuristicsMTest extends SpringUnitTest {

	@Autowired private SparkCsvReader sparkCsvReader;

	private Dataset<Row>[] dts;

	@Before
	public void setUp() throws IOException {
		ClassPathResource resource = new ClassPathResource("/files/paper-sample.csv", getClass());
		File file = new File(resource.getURL().getPath());

		Map<String, String> options = new HashMap<>();
		options.put("header", "true");

		Dataset<Row> is = sparkCsvReader.read(file, options);
		dts = DecisionTableFactory.extractDecisionTables(is);
	}

	@After
	public void tearDown() {
		dts = null;
	}

	@Test
	public void test() {
		HeuristicsM.calculateDecisionRules(dts);
	}

}