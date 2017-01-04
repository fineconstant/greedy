package org.kduda.greedy.service.storage;

import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Service
public class MongoStorageService implements StorageService {

	@Override
	public void init() {
		System.out.println("mongo init");
	}

	@Override
	public void store(MultipartFile file) {

	}

	@Override
	public Stream<Path> loadAll() {
		return Stream.of(Paths.get("first"), Paths.get("second"));
	}

	@Override
	public Path load(String filename) {
		return null;
	}

	@Override
	public Resource loadAsResource(String filename) {
		return null;
	}

	@Override
	public void deleteAll() {

	}
}
