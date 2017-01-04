package org.kduda.greedy.controller;

import org.kduda.greedy.exception.StorageFileNotFoundException;
import org.kduda.greedy.service.storage.StorageService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.IOException;
import java.util.stream.Collectors;

@Controller
public class FileStorageController {

	//TODO: lombok: private val storageService;
	private final StorageService storageService;

	public FileStorageController(@Qualifier("mongoStorageService") StorageService storageService) {
		this.storageService = storageService;
	}

	@GetMapping("files")
	public String listUploadedFiles(Model model) throws IOException {

		model.addAttribute("files", storageService
			.loadAll()
			.map(path -> MvcUriComponentsBuilder
				.fromMethodName(FileStorageController.class, "serveFile", path.getFileName().toString())
				.build().toString())
			.collect(Collectors.toList()));

		return "uploadForm";
	}

	@GetMapping("/files/{filename:.+}")
	@ResponseBody
	public ResponseEntity<Resource> serveFile(@PathVariable String filename) {

		Resource file = storageService.loadAsResource(filename);
		return ResponseEntity
			.ok()
			.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getFilename() + "\"")
			.body(file);
	}

	@PostMapping("files")
	public String handleFileUpload(@RequestParam("file") MultipartFile file,
								   RedirectAttributes redirectAttributes) {

		storageService.store(file);
		redirectAttributes.addFlashAttribute("message", "You successfully uploaded " + file.getOriginalFilename() + "!");

		return "redirect:/files";
	}

	@ExceptionHandler(StorageFileNotFoundException.class)
	public ResponseEntity handleStorageFileNotFound(StorageFileNotFoundException ex) {
		return ResponseEntity.notFound().build();
	}

}