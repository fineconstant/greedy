package org.kduda.greedy.repository;

import com.mongodb.gridfs.GridFSDBFile;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.kduda.greedy.domain.FileContentTypes;
import org.kduda.greedy.domain.GreedyTableTypes;
import org.kduda.greedy.exception.StorageException;
import org.kduda.greedy.model.FileModel;
import org.kduda.greedy.service.storage.MongoGridFsStorageService;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Repository;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// TODO: refactor when there is decision table or association rule file repository - make it more generic
@Repository
public class InfoSystemCsvRepository implements CsvRepository {

	private final MongoGridFsStorageService storageService;

	public InfoSystemCsvRepository(MongoGridFsStorageService storageService) {
		this.storageService = storageService;
	}

	@Override
	public void store(MultipartFile file) {
		try {
			if (file.isEmpty()) {
				throw new StorageException("Failed to store empty file " + file.getOriginalFilename());
			}
			Map<String, String> metadata = new HashMap<>();
			metadata.put("type", GreedyTableTypes.INFOTMATION_SYSTEM.getType());

			storageService.storeFile(file.getInputStream(), file.getOriginalFilename(), FileContentTypes.CSV.getType(), metadata);

		} catch (IOException e) {
			throw new StorageException("Failed to store file " + file.getOriginalFilename(), e);
		}
	}

	@Override
	public List<FileModel> listAll() {
		return storageService.findFilesByType(GreedyTableTypes.INFOTMATION_SYSTEM.getType())
							 .stream()
							 .map(file -> new FileModel(file.getFilename(), file.getId().toString()))
							 .collect(Collectors.toList());
	}

	@Override
	public Pair<String, Resource> loadResourceById(@NonNull String id) {
		Optional<GridFSDBFile> oFile = storageService.findFileById(id);
		return loadAsResource(oFile);
	}

	@Override
	public Pair<String, Resource> loadResourceByFilename(@NonNull String filename) {
		Optional<GridFSDBFile> oFile = storageService.findFileByName(filename);
		return loadAsResource(oFile);
	}

	private Pair<String, Resource> loadAsResource(Optional<GridFSDBFile> oFile) {
		Pair<String, Resource> result = null;

		if (oFile.isPresent()) {
			String filename = oFile.get().getFilename();
			Resource resource = new InputStreamResource(oFile.get().getInputStream());

			result = Pair.of(filename, resource);
		}
		return result;
	}

	@Override
	public void deleteById(@NonNull String id) {
		storageService.deleteById(id);
	}

	@Override
	public void deleteByFilename(@NonNull String name) {
		storageService.deleteByFilename(name);
	}

	@Override
	public void deleteAll() {
		storageService.deleteByType(GreedyTableTypes.INFOTMATION_SYSTEM.getType());
	}

	@Override
	public void init() {
	}
}
