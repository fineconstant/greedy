package org.kduda.greedy.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class ExploreController {

	@GetMapping("/explore/{id}")
	public String explore() {
		return "explore";
	}
}
