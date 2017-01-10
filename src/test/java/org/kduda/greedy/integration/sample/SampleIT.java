package org.kduda.greedy.integration.sample;

import org.junit.Test;
import org.kduda.greedy.integration.SpringIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

public class SampleIT extends SpringIntegrationTest {
	@Test
	public void shouldPass() {
		assertThat(true).isTrue();
	}

	@Test
	public void shouldFail() {
		assertThat(true).isFalse();
	}
}
