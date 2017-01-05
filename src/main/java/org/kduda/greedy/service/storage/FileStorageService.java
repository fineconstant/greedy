package org.kduda.greedy.service.storage;

import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;
import org.springframework.core.io.Resource;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface FileStorageService {

	Optional<GridFSFile> storeFile(Resource resource, String contentType, Map<String, String> metadata);

	GridFSFile findFilesByName(String name);

	GridFSFile findFilesById(String id);

	List<? extends GridFSFile> findFilesByType(String type);

	void deleteById(String id);

	void deleteByFilename(String filename);
}
