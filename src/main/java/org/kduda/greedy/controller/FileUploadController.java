package org.kduda.greedy.controller;

import org.apache.commons.lang3.tuple.Pair;
import org.kduda.greedy.exception.StorageFileNotFoundException;
import org.kduda.greedy.repository.CsvRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.IOException;

@Controller
public class FileUploadController {

	private final CsvRepository csvRepository;

	public FileUploadController(CsvRepository csvRepository) {
		this.csvRepository = csvRepository;
	}

	@GetMapping("/files")
	public String listFiles(Model model) throws IOException {
		model.addAttribute("files", csvRepository.listAll());
		return "uploadForm";
	}

	@GetMapping("/files/{id:.+}")
	@ResponseBody
	public ResponseEntity<Resource> serveFile(@PathVariable String id) {
		Pair<String, Resource> file = csvRepository.loadResourceById(id);
		if (file != null) {
			return ResponseEntity
				.ok()
				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file.getKey() + "\"")
				.body(file.getValue());
		}
		return ResponseEntity.notFound().build();
	}

	@PostMapping("/files")
	public String handleFileUpload(@RequestParam("file") MultipartFile file,
								   RedirectAttributes redirectAttributes) {
		csvRepository.store(file);
		redirectAttributes.addFlashAttribute("message", "You successfully uploaded " + file.getOriginalFilename() + "!");

		return "redirect:/files";
	}

	@ExceptionHandler(StorageFileNotFoundException.class)
	public ResponseEntity handleStorageFileNotFound(StorageFileNotFoundException ex) {
		return ResponseEntity.notFound().build();
	}

}