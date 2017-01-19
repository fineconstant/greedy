package org.kduda.greedy.unit.algorithm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.kduda.greedy.spark.reader.csv.SparkCsvReader;
import org.kduda.greedy.unit.SpringUnitTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;

public class DecisionTableFactoryTest extends SpringUnitTest {
	@Autowired private SparkCsvReader sparkCsvReader;
	private Dataset<Row> informationSystem;

	@Before
	public void before() throws IOException {
		ClassPathResource resource = new ClassPathResource("/files/paper-sample.csv", getClass());
		File file = new File(resource.getURL().getPath());
		informationSystem = sparkCsvReader.read(file, null);
	}

	@Test
	public void shouldExtractDecisionTables() {

	}
}
