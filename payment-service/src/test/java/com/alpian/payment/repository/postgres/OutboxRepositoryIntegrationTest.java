package com.alpian.payment.repository.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.alpian.payment.repository.OutboxMessage;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutboxRepositoryIntegrationTest extends PostgresTestBase {

  private static final Instant NOW = Instant.parse("2026-10-08T09:00:00Z");

  @Test
  @DisplayName("two concurrent claims take disjoint batches, never the same row")
  void concurrentClaimsAreDisjoint() throws Exception {
    // What lets every replica run a relay with no coordination. Relay A claims a batch and holds
    // its transaction open; relay B, claiming at the same moment, must skip A's locked rows rather
    // than wait for them or take them too.
    enqueue(10);
    CountDownLatch aHasClaimed = new CountDownLatch(1);
    CountDownLatch bHasClaimed = new CountDownLatch(1);

    CompletableFuture<List<Long>> relayA =
        CompletableFuture.supplyAsync(
            () ->
                transactions.execute(
                    status -> {
                      List<Long> ids = ids(outbox.claimBatch(4, NOW));
                      aHasClaimed.countDown();
                      await(bHasClaimed); // keep the locks held while B claims
                      return ids;
                    }));

    await(aHasClaimed);
    List<Long> claimedByB = transactions.execute(status -> ids(outbox.claimBatch(4, NOW)));
    bHasClaimed.countDown();
    List<Long> claimedByA = relayA.get(10, TimeUnit.SECONDS);

    assertThat(claimedByA).hasSize(4);
    assertThat(claimedByB).hasSize(4);
    Set<Long> overlap = new HashSet<>(claimedByA);
    overlap.retainAll(claimedByB);
    assertThat(overlap).as("rows claimed by both relays").isEmpty();
  }

  @Test
  @DisplayName("claims return the oldest due rows first")
  void claimsInCommitOrder() {
    enqueue(5);

    List<Long> ids = transactions.execute(status -> ids(outbox.claimBatch(5, NOW)));

    assertThat(ids).isSorted();
  }

  @Test
  @DisplayName("published, parked and not-yet-due rows are not claimed")
  void claimsOnlyDueRows() {
    enqueue(4);
    List<Long> all = transactions.execute(status -> ids(outbox.claimBatch(10, NOW)));

    transactions.executeWithoutResult(
        status -> {
          outbox.markPublished(List.of(all.get(0)), NOW);
          outbox.park(all.get(1), "poison", NOW);
          outbox.recordFailure(all.get(2), "broker down", NOW.plusSeconds(60));
        });

    List<Long> due = transactions.execute(status -> ids(outbox.claimBatch(10, NOW)));
    assertThat(due).containsExactly(all.get(3));

    // Once its backoff has elapsed, the failed row becomes due again.
    List<Long> later =
        transactions.execute(status -> ids(outbox.claimBatch(10, NOW.plusSeconds(61))));
    assertThat(later).containsExactlyInAnyOrder(all.get(2), all.get(3));
  }

  @Test
  @DisplayName("the backlog counts pending rows only, and reports the oldest")
  void reportsTheBacklog() {
    enqueue(3);
    List<Long> all = transactions.execute(status -> ids(outbox.claimBatch(10, NOW)));
    transactions.executeWithoutResult(status -> outbox.markPublished(List.of(all.get(0)), NOW));

    var backlog = outbox.backlog();

    assertThat(backlog.pending()).isEqualTo(2);
    assertThat(backlog.oldestCreatedAt()).isPresent();
  }

  @Test
  @DisplayName("enqueue refuses to run outside a transaction, where it would be a dual write")
  void enqueueRequiresATransaction() {
    assertThatThrownBy(() -> outbox.enqueue(message()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("payment transaction");
  }

  @Test
  void claimRequiresATransaction() {
    assertThatThrownBy(() -> outbox.claimBatch(1, NOW))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("row locks");
  }

  @Test
  void truncatesOverlongErrors() {
    enqueue(1);
    long id = transactions.execute(status -> outbox.claimBatch(1, NOW).get(0).id());

    transactions.executeWithoutResult(status -> outbox.recordFailure(id, "x".repeat(5000), NOW));

    String stored =
        jdbc.sql("SELECT last_error FROM outbox WHERE id = :id")
            .param("id", id)
            .query(String.class)
            .single();
    assertThat(stored).hasSize(1000);
  }

  private void enqueue(int count) {
    transactions.executeWithoutResult(
        status -> {
          for (int i = 0; i < count; i++) {
            outbox.enqueue(message());
          }
        });
  }

  private static OutboxMessage message() {
    return OutboxMessage.pending(
        UUID.randomUUID(), UUID.randomUUID().toString(), "test", new byte[] {1, 2, 3}, NOW);
  }

  private static List<Long> ids(List<OutboxMessage> messages) {
    return messages.stream().map(OutboxMessage::id).toList();
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new AssertionError("timed out waiting for the other relay");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
