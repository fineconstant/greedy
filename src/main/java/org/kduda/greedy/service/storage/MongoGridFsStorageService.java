package org.kduda.greedy.service.storage;

import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFSFile;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.whereFilename;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.whereMetaData;

@Slf4j
@Service
public class MongoGridFsStorageService implements FileStorageService {

	private final GridFsOperations operations;

	public MongoGridFsStorageService(GridFsOperations gridFsOperations) {
		this.operations = gridFsOperations;
	}

	@Override
	public Optional<GridFSFile> storeFile(@NonNull Resource resource, String contentType, Map<String, String> metadata) {

		Optional<GridFSFile> result = Optional.empty();
		try {
			result = Optional.of(operations.store(resource.getInputStream(), resource.getFilename(), contentType, metadata));
		} catch (IOException e) {
			log.error("Error getting input stream from file", e);
		}

		try {
			result.ifPresent(GridFSFile::validate);
		} catch (MongoException e) {
			log.error("File validation fail", e);
		}

		return result;
	}

	@Override
	public GridFSFile findFilesByName(@NonNull String name) {
		return operations.findOne(query(whereFilename().is(name)));
	}

	@Override
	public GridFSFile findFilesById(@NonNull String id) {
		return operations.findOne(query(whereMetaData("_id").is(id)));
	}

	@Override
	public List<? extends GridFSFile> findFilesByType(@NonNull String type) {
		return operations.find(query(whereMetaData("type").is(type)));
	}

	@Override
	public void deleteById(@NonNull String id) {
		operations.delete(query(where("_id").is(id)));
	}

	@Override
	public void deleteByFilename(@NonNull String filename) {
		operations.delete(query(whereFilename().is(filename)));
	}
}