package org.kduda.greedy.repository;

import org.apache.commons.lang3.tuple.Pair;
import org.kduda.greedy.model.FileModel;
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public interface CsvRepository {

	void init();

	void store(MultipartFile file);

	List<FileModel> listAll();

	Pair<String, Resource> loadResourceById(String Id);

	Pair<String, Resource> loadResourceByFilename(String filename);

	void deleteById(String id);

	void deleteByFilename(String name);

	void deleteAll();

}
