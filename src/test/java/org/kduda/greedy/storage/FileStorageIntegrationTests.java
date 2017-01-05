package org.kduda.greedy.storage;

import org.junit.Test;
import org.kduda.greedy.SpringIntegrationTest;
import org.kduda.greedy.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Matchers.any;

public class FileStorageIntegrationTests extends SpringIntegrationTest {

	@Autowired
	private TestRestTemplate restTemplate;

	@MockBean
	@Qualifier("mongoGridFsStorageService")
	private StorageService storageService;

	@LocalServerPort
	private int port;

	@Test
	public void shouldUploadFile() throws Exception {
		ClassPathResource resource = new ClassPathResource("/storage-integration-test-file.txt", getClass());

		MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
		map.add("file", resource);
		ResponseEntity<String> response = restTemplate.postForEntity("/files", map, String.class);

		assertThat(response.getStatusCode()).isEqualByComparingTo(HttpStatus.FOUND);
		assertThat(response.getHeaders().getLocation().toString()).startsWith("http://localhost" + "/files");
		then(storageService).should().store(any(MultipartFile.class));
	}

	@Test
	public void shouldDownloadFile() throws Exception {
		ClassPathResource resource = new ClassPathResource("/storage-integration-test-file.txt", getClass());
		given(storageService.loadAsResource("storage-integration-test-file.txt")).willReturn(resource);

		ResponseEntity<String> response = restTemplate.getForEntity("/files/{filename}", String.class,
																	"storage-integration-test-file.txt");

		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		assertThat(response.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION))
			.isEqualTo("attachment; filename=\"storage-integration-test-file.txt\"");
		assertThat(response.getBody()).isEqualTo("Storage Integration Tests");
	}
}
