package io.orderplatform.dlq;

import io.orderplatform.dlq.DeadLetterInspectorAndReplayer.DeadLetterEntry;
import io.orderplatform.events.TopicNames;
import io.orderplatform.events.avro.InventoryReserved;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DlqCliOptionsAndReplayTest {

    @Test
    void parsesInspectAndReplayFlagsAndRejectsMissingTopic() {
        DlqCliOptions inspect = DlqCliOptions.parse(new String[]{
                "inspect", "--topic", "inventory.reserved.v1.dlt", "--key", "ord-123", "--format", "json"
        });
        assertThat(inspect.command()).isEqualTo("inspect");
        assertThat(inspect.topic()).isEqualTo("inventory.reserved.v1.dlt");
        assertThat(inspect.keyFilter()).isEqualTo("ord-123");
        assertThat(inspect.format()).isEqualTo("json");

        DlqCliOptions replay = DlqCliOptions.parse(new String[]{
                "replay", "--topic=inventory.reserved.v1.dlt", "--override-postal-code", "98101", "--dry-run"
        });
        assertThat(replay.command()).isEqualTo("replay");
        assertThat(replay.overridePostalCode()).isEqualTo("98101");
        assertThat(replay.dryRun()).isTrue();

        assertThatThrownBy(() -> DlqCliOptions.parse(new String[]{"inspect"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--topic is required");
    }

    @Test
    void replayStripsRetrySuffixesOverridesPostalCodeAndPreservesTraceparentWithoutDltHeaders() {
        InventoryReserved poison = new InventoryReserved(
                "evt-1", "ord-p1", "res-1", "SKU-KB", 1, "POISON-ALWAYS", "a@b.com", Instant.now().toEpochMilli());

        DeadLetterEntry entry = new DeadLetterEntry(
                "inventory.reserved.v1.dlt", 2, 14L, Instant.now().toEpochMilli(),
                "ord-p1", true, InventoryReserved.class.getName(), poison.toString(),
                "inventory.reserved.v1.retry-2", "TransientCarrierException", "503",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                poison, new byte[]{0, 0, 0, 0, 1}
        );

        DlqCliOptions options = DlqCliOptions.parse(new String[]{
                "replay", "--topic", "inventory.reserved.v1.dlt", "--override-postal-code", "98101"
        });

        assertThat(DeadLetterInspectorAndReplayer.resolveTargetTopic(options, entry))
                .isEqualTo(TopicNames.INVENTORY_RESERVED);

        InventoryReserved patched = (InventoryReserved) DeadLetterInspectorAndReplayer.prepareReplayPayload(entry, options);
        assertThat(patched.getShippingPostalCode()).isEqualTo("98101");

        RecordHeaders replayHeaders = new RecordHeaders();
        DeadLetterInspectorAndReplayer.populateReplayHeaders(replayHeaders, entry);
        assertThat(replayHeaders.lastHeader("traceparent")).isNotNull();
        assertThat(replayHeaders.lastHeader(DeadLetterInspectorAndReplayer.REPLAYED_AT_HEADER)).isNotNull();
        assertThat(replayHeaders.lastHeader("kafka_dlt-original-topic")).isNull();
    }
}
