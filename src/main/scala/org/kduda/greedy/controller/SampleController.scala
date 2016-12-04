package org.kduda.greedy.controller

import org.springframework.web.bind.annotation.{GetMapping, RestController}

@RestController
class SampleController {

  @GetMapping(Array("/"))
  def defaultMapping = "scala default mapping"

  @GetMapping(Array("/test"))
  def test(): Boolean = {
    false
  }
}
