package io.orderplatform.dlq;

import java.util.List;

public final class DlqCliApplication {

    private DlqCliApplication() {
    }

    public static void main(String[] args) throws Exception {
        int code = execute(args);
        if (code != 0) {
            System.exit(code);
        }
    }

    public static int execute(String[] args) throws Exception {
        DlqCliOptions options;
        try {
            options = DlqCliOptions.parse(args);
        } catch (IllegalArgumentException ex) {
            System.err.println(ex.getMessage());
            return 2;
        }

        DeadLetterInspectorAndReplayer engine = new DeadLetterInspectorAndReplayer();
        if ("inspect".equals(options.command())) {
            List<DeadLetterInspectorAndReplayer.DeadLetterEntry> entries = engine.inspect(options);
            System.out.print(engine.formatInspectionOutput(entries, options.format()));
            return 0;
        }

        DeadLetterInspectorAndReplayer.ReplayResult result = engine.replay(options);
        System.out.printf(
                "DLQ Replay Summary [topic=%s dryRun=%s]: inspected=%d replayed=%d skippedCorrupt=%d keys=%s%n",
                options.topic(), options.dryRun(),
                result.inspectedCount(), result.replayedCount(),
                result.skippedCorruptCount(), result.replayedKeys()
        );
        return 0;
    }
}
