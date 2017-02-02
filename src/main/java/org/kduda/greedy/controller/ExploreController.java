package org.kduda.greedy.controller;

import org.kduda.greedy.repository.data.FileRepository;
import org.kduda.greedy.repository.rules.RulesRepository;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@Controller
public class ExploreController {

	private final FileRepository fileRepository;

	private final RulesRepository rulesRepository;

	public ExploreController(FileRepository fileRepository, RulesRepository rulesRepository) {
		this.fileRepository = fileRepository;
		this.rulesRepository = rulesRepository;
	}

	@GetMapping("/explore/{id}")
	public String explore(@PathVariable("id") String id, Model model) {
		model.addAttribute("dataFile", fileRepository.listById(id));
		model.addAttribute("files", rulesRepository.listByParent(id));
		return "explore";
	}
}
