package com.alpian.payment.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Supplies the clock used for journal timestamps. */
@Configuration
public class TimeConfiguration {

  /**
   * Injected rather than read through {@code Instant.now()} so that tests can fix time and assert
   * on recorded timestamps, instead of asserting a range and hoping.
   *
   * <p>UTC explicitly: the journal columns are {@code TIMESTAMPTZ} and nothing downstream should
   * depend on the host's zone.
   */
  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }
}
