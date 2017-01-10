package org.kduda.greedy.unit.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.kduda.greedy.spark.reader.CsvResourceSparkReader;
import org.kduda.greedy.spark.reader.SparkReader;
import org.kduda.greedy.unit.SpringUnitTest;
import org.springframework.core.io.ClassPathResource;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvInputTest extends SpringUnitTest {
	private SparkReader reader = null;
	private Map<String, String> options = null;

	@Before
	public void before() {
		reader = new CsvResourceSparkReader();
		options = new HashMap<>();
	}

	@Test
	public void shouldReadCsvFile() throws IOException {
		ClassPathResource resource = new ClassPathResource("/paper-sample.csv", getClass());

		Dataset<Row> csv = reader.read(resource, null);

		assertThat(csv).isNotNull();
	}

	@Test
	public void shouldReadCsvFileWithOptions() throws IOException {
		ClassPathResource resource = new ClassPathResource("/paper-sample.csv", getClass());

		options.put("headers", "true");
		Dataset<Row> csv = reader.read(resource, options);

		assertThat(csv).isNotNull();
	}

	@Test
	public void shouldReadCsvFileWithEmptyOptions() throws IOException {
		ClassPathResource resource = new ClassPathResource("/paper-sample.csv", getClass());

		Dataset<Row> csv = reader.read(resource, options);

		assertThat(csv).isNotNull();
	}
}
