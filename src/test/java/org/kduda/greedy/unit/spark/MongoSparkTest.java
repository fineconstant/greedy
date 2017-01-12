package org.kduda.greedy.unit.spark;

import com.mongodb.gridfs.GridFSDBFile;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.kduda.greedy.service.storage.MongoGridFsStorageService;
import org.kduda.greedy.spark.GreedySparkInstance;
import org.kduda.greedy.unit.SpringUnitTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;

@Slf4j
public class MongoSparkTest extends SpringUnitTest {

	@Autowired private MongoGridFsStorageService storageService;

	/**
	 * TODO:
	 * in integration test:
	 * before class: upload file to mongo
	 * after class: delete file in mongo
	 */

	@Test
	public void testSaveToMongo() {
		// TODO: get by id
		GridFSDBFile file = storageService.findFileByName("sample.csv").get();

		File storageFile = new File("tmp-storage/" + file.getFilename());

		long written = 0L;
		try {
			written = file.writeTo(storageFile);
		} catch (IOException e) {
			log.error("Could write to file:" + storageFile.getAbsolutePath(), e);
		}
		log.info("Written " + written + " bytes into " + storageFile.getAbsolutePath());

		Dataset<Row> data = GreedySparkInstance.sql().read().option("header", true).csv(storageFile.getPath());
		data.cache().show();
		boolean isDeleted = storageFile.delete();
		log.info(storageFile.getAbsolutePath() + " deleted: " + isDeleted);
		data.show();
	}
}
