import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import type { Role } from "../components/DeleteModal";
import SourceDetail from "./SourceDetail";

const SOURCE_NAME = "dbz-mssql1-students";

const SOURCE: client.Source = {
  name: SOURCE_NAME,
  class: "io.debezium.connector.sqlserver.SqlServerConnector",
  paused: false,
  state: "RUNNING",
  cr_kind: "KafkaConnector",
};

// A kafka-ingest source is also a bare KafkaConnector CR (cr_kind alone
// can't distinguish it from a CDC connector -- see the ingest-config 400
// gating above), but its `class` is the Iceberg sink connector, never a
// `io.debezium.connector.*` class. Snapshot-lifecycle gating (Task 10) uses
// that distinction to hide re-snapshot/progress/enable-snapshots (Debezium-
// only concepts) while still showing Start/Stop (which apply to any
// connector CR).
const KAFKA_INGEST_SOURCE: client.Source = {
  name: "kafka-ingest-nginx",
  class: "org.apache.iceberg.connect.IcebergSinkConnector",
  paused: false,
  state: "RUNNING",
  cr_kind: "KafkaConnector",
};

const SPARK_SOURCE_NAME = "s3-batch-invoices";

const SPARK_SOURCE: client.Source = {
  name: SPARK_SOURCE_NAME,
  class: "ScheduledSparkApplication",
  paused: false,
  state: "Ready",
  cr_kind: "ScheduledSparkApplication",
  spark: {
    source: SPARK_SOURCE_NAME,
    target_ns: "rawlake",
    target_table: "invoices",
    s3_bucket: "raw-bucket",
    s3_prefix: "invoices/",
    file_format: "parquet",
    cron: "0 * * * *",
  },
};

const STATUS: client.StatusResponse = {
  connectors: [
    {
      name: SOURCE_NAME,
      state: "RUNNING",
      maintenance: false,
      dlq: false,
      lag: 42,
      reachable: true,
    },
  ],
  reachable: true,
};

function renderDetail(role: Role, sourceName: string = SOURCE_NAME) {
  return render(
    <MemoryRouter initialEntries={[`/sources/${sourceName}`]}>
      <Routes>
        <Route path="/sources/:name" element={<SourceDetail role={role} />} />
        {/* Deleting navigates back to "/" -- give it somewhere to land so
         * React Router doesn't warn about an unmatched location. */}
        <Route path="/" element={<p>Sources list</p>} />
      </Routes>
    </MemoryRouter>,
  );
}

async function openDeleteModal() {
  fireEvent.click(await screen.findByRole("button", { name: /delete source/i }));
}

function mockLoad() {
  vi.spyOn(client, "getSource").mockResolvedValue(SOURCE);
  vi.spyOn(client, "getStatus").mockResolvedValue(STATUS);
  vi.spyOn(client, "getSourceConnectors").mockResolvedValue({ connectors: [] });
  // Ingestion/collector config section (Task 9) fetches lazily -- only on
  // open -- so most existing tests never trigger this call. Still spied
  // here with a safe default so any test that does open it doesn't hit an
  // unmocked network call.
  vi.spyOn(client, "getIngestConfig").mockResolvedValue({
    external_bootstrap: "kafka.example:9094",
    topic: "nginx-logs",
    disposition: "event",
    authoritative_fqn: "rawlake.nginx_raw.nginx_logs",
    producer: {
      user: "nginx-producer",
      mechanism: "SCRAM-SHA-512",
      password: null,
      secret_ref: "nginx-producer",
    },
    expected_json: null,
    snippets: { fluentbit: "f", vector: "v", logstash: "l", generic: "g" },
  });
  // Dead-letter records section (Task 5) fetches lazily -- only when the
  // per-connector "Dead-letter records" button is clicked -- so most
  // existing tests never trigger this call. Still spied here with a safe
  // default so any test that does open it doesn't hit an unmocked network
  // call.
  vi.spyOn(client, "getConnectorDlq").mockResolvedValue({ has_dlq: false, hint: "no DLQ" });
}

describe("SourceDetail", () => {
  it("shows the loaded source's config/status/lag/dlq", async () => {
    mockLoad();

    renderDetail("ADMIN");

    expect(await screen.findByText(SOURCE_NAME)).toBeInTheDocument();
    expect(
      screen.getByText("io.debezium.connector.sqlserver.SqlServerConnector"),
    ).toBeInTheDocument();
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
  });

  it("defaults the delete modal to pipeline_only and calls deleteSource with that mode", async () => {
    mockLoad();
    const deleteSpy = vi.spyOn(client, "deleteSource").mockResolvedValue({
      ok: true,
      name: SOURCE_NAME,
      mode: "pipeline_only",
    });

    renderDetail("ADMIN");
    await openDeleteModal();

    expect(screen.getByLabelText(/yalnızca pipeline/i)).toBeChecked();

    fireEvent.click(screen.getByRole("button", { name: /^delete$/i }));

    await waitFor(() => expect(deleteSpy).toHaveBeenCalledTimes(1));
    expect(deleteSpy).toHaveBeenCalledWith(SOURCE_NAME, "pipeline_only");
  });

  it("keeps the with-data delete disabled until the exact source name is typed, then deletes with_data", async () => {
    mockLoad();
    const deleteSpy = vi.spyOn(client, "deleteSource").mockResolvedValue({
      ok: true,
      name: SOURCE_NAME,
      mode: "with_data",
    });

    renderDetail("ADMIN");
    await openDeleteModal();

    fireEvent.click(screen.getByLabelText(/veriyle birlikte/i));

    const confirmButton = screen.getByRole("button", { name: /^delete$/i });
    expect(confirmButton).toBeDisabled();

    fireEvent.change(screen.getByLabelText(/type.*to confirm/i), {
      target: { value: "not-the-right-name" },
    });
    expect(confirmButton).toBeDisabled();
    expect(deleteSpy).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText(/type.*to confirm/i), {
      target: { value: SOURCE_NAME },
    });
    expect(confirmButton).not.toBeDisabled();

    fireEvent.click(confirmButton);

    await waitFor(() => expect(deleteSpy).toHaveBeenCalledTimes(1));
    expect(deleteSpy).toHaveBeenCalledWith(SOURCE_NAME, "with_data");
  });

  it("hides the with-data delete option for a non-admin role", async () => {
    mockLoad();
    const deleteSpy = vi.spyOn(client, "deleteSource");

    renderDetail("ANALYST");
    await openDeleteModal();

    expect(screen.getByLabelText(/yalnızca pipeline/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/veriyle birlikte/i)).not.toBeInTheDocument();

    // Only the safe default is reachable -- confirming still works, still pipeline_only.
    fireEvent.click(screen.getByRole("button", { name: /^delete$/i }));
    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith(SOURCE_NAME, "pipeline_only"));
  });

  it("pauses a running source and resumes a paused one", async () => {
    mockLoad();
    const pauseSpy = vi
      .spyOn(client, "pauseSource")
      .mockResolvedValue({ ok: true, name: SOURCE_NAME, paused: true });

    renderDetail("ADMIN");

    fireEvent.click(await screen.findByRole("button", { name: /^pause$/i }));

    await waitFor(() => expect(pauseSpy).toHaveBeenCalledWith(SOURCE_NAME));
    expect(await screen.findByRole("button", { name: /^resume$/i })).toBeInTheDocument();
  });

  it("shows the JSON config textarea for a connector source and saves via patchSource", async () => {
    mockLoad();
    const patchSpy = vi
      .spyOn(client, "patchSource")
      .mockResolvedValue({ ok: true, name: SOURCE_NAME });

    renderDetail("ADMIN");

    fireEvent.click(await screen.findByRole("button", { name: /^edit$/i }));

    expect(await screen.findByLabelText(/config \(json\)/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/cron/i)).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/config \(json\)/i), {
      target: { value: '{"foo":"bar"}' },
    });
    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));

    await waitFor(() => expect(patchSpy).toHaveBeenCalledWith(SOURCE_NAME, { foo: "bar" }));
  });

  it("shows a prefilled field form for a spark-batch source and saves via editSparkSource", async () => {
    vi.spyOn(client, "getSource").mockResolvedValue(SPARK_SOURCE);
    vi.spyOn(client, "getStatus").mockResolvedValue({ connectors: [], reachable: true });
    vi.spyOn(client, "getSourceConnectors").mockResolvedValue({ connectors: [] });
    const editSparkSpy = vi
      .spyOn(client, "editSparkSource")
      .mockResolvedValue({ ok: true, name: SPARK_SOURCE_NAME });

    renderDetail("ADMIN", SPARK_SOURCE_NAME);

    fireEvent.click(await screen.findByRole("button", { name: /^edit$/i }));

    expect(screen.queryByLabelText(/config \(json\)/i)).not.toBeInTheDocument();

    const cronInput = (await screen.findByLabelText(/cron/i)) as HTMLInputElement;
    const bucketInput = screen.getByLabelText(/s3 bucket/i) as HTMLInputElement;
    const prefixInput = screen.getByLabelText(/s3 prefix/i) as HTMLInputElement;
    const formatSelect = screen.getByLabelText(/file format/i) as HTMLSelectElement;

    expect(cronInput.value).toBe("0 * * * *");
    expect(bucketInput.value).toBe("raw-bucket");
    expect(prefixInput.value).toBe("invoices/");
    expect(formatSelect.value).toBe("parquet");

    // The file_format field is a constrained <select>, not free text -- assert
    // exactly the three valid formats are offered (no room for an invalid
    // "csv"/"orc" value to reach the PATCH body).
    const formatOptions = Array.from(formatSelect.options).map((o) => o.value);
    expect(formatOptions).toEqual(["parquet", "json", "avro"]);

    fireEvent.change(cronInput, { target: { value: "*/15 * * * *" } });
    fireEvent.change(bucketInput, { target: { value: "new-bucket" } });
    fireEvent.change(prefixInput, { target: { value: "new-invoices/" } });
    fireEvent.change(formatSelect, { target: { value: "json" } });

    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));

    await waitFor(() =>
      expect(editSparkSpy).toHaveBeenCalledWith(SPARK_SOURCE_NAME, {
        source: SPARK_SOURCE_NAME,
        kind: "batch",
        type: "s3",
        db: "-",
        table: "-",
        target_ns: "rawlake",
        target_table: "invoices",
        s3_bucket: "new-bucket",
        s3_prefix: "new-invoices/",
        file_format: "json",
        cron: "*/15 * * * *",
      }),
    );
  });

  it("shows an error message when the source fails to load", async () => {
    vi.spyOn(client, "getSource").mockRejectedValue(new Error("not found"));
    vi.spyOn(client, "getStatus").mockResolvedValue({ connectors: [], reachable: true });

    renderDetail("ADMIN");

    expect(await screen.findByText(/not found/i)).toBeInTheDocument();
  });

  it("restarts a connector and shows confirmation", async () => {
    vi.spyOn(client, "getSource").mockResolvedValue(SOURCE);
    vi.spyOn(client, "getStatus").mockResolvedValue(STATUS);
    vi.spyOn(client, "getSourceConnectors").mockResolvedValue({
      connectors: [{ name: SOURCE_NAME, role: "source", kind: "io...Debezium", state: "RUNNING" }],
    });
    const restartSpy = vi
      .spyOn(client, "restartConnector")
      .mockResolvedValue({ ok: true, name: SOURCE_NAME });

    renderDetail("ADMIN");

    fireEvent.click(await screen.findByRole("button", { name: /^restart$/i }));
    await waitFor(() => expect(restartSpy).toHaveBeenCalledWith(SOURCE_NAME, expect.any(Object)));
    expect(await screen.findByText(/restart triggered/i)).toBeInTheDocument();
  });

  it("opens the debug panel and shows the failure trace + oc command", async () => {
    vi.spyOn(client, "getSource").mockResolvedValue(SOURCE);
    vi.spyOn(client, "getStatus").mockResolvedValue(STATUS);
    vi.spyOn(client, "getSourceConnectors").mockResolvedValue({
      connectors: [{ name: SOURCE_NAME, role: "source", kind: null, state: "FAILED" }],
    });
    vi.spyOn(client, "getConnectorDebug").mockResolvedValue({
      name: SOURCE_NAME,
      state: "FAILED",
      tasks: [{ id: 0, state: "FAILED", worker_id: "10.0.0.1:8083", trace: "boom-stacktrace" }],
      logs_hint: {
        namespace: "lakehouse",
        connect_pods_selector: "strimzi.io/cluster=connect,strimzi.io/kind=KafkaConnect",
        search_terms: [SOURCE_NAME, "task-0"],
        oc_command: "oc logs -n lakehouse -l ... | grep 'src'",
        external_link: null,
      },
    });

    renderDetail("ADMIN");

    fireEvent.click(await screen.findByRole("button", { name: /^debug$/i }));
    expect(await screen.findByText(/boom-stacktrace/)).toBeInTheDocument();
    expect(screen.getByText(/oc logs -n lakehouse/)).toBeInTheDocument();
  });

  it("shows dead-letter records for a connector with a DLQ", async () => {
    vi.spyOn(client, "getSource").mockResolvedValue(SOURCE);
    vi.spyOn(client, "getStatus").mockResolvedValue(STATUS);
    vi.spyOn(client, "getSourceConnectors").mockResolvedValue({
      connectors: [{ name: SOURCE_NAME, role: "source", kind: null, state: "RUNNING" }],
    });
    vi.spyOn(client, "getConnectorDlq").mockResolvedValue({
      has_dlq: true,
      topic: "s.dlq",
      count: 2,
      returned: 1,
      records: [
        {
          ts: 1730000000000,
          error_class: "DataException",
          error_message: "boom",
          source_topic: "orders",
          source_partition: 2,
          source_offset: 99,
          value_preview: "{...}",
        },
      ],
    });

    renderDetail("ADMIN");

    fireEvent.click(await screen.findByRole("button", { name: /dead-letter|dlq/i }));
    expect(await screen.findByText(/DataException/)).toBeInTheDocument();
    expect(screen.getByText(/boom/)).toBeInTheDocument();
    expect(screen.getByText(/orders:2@99/)).toBeInTheDocument();
    expect(screen.getByText(/showing last 1 of 2/i)).toBeInTheDocument();
    expect(screen.getByText(/sample — may contain data/i)).toBeInTheDocument();
  });

  it("shows a clean empty state when DLQ count is 0", async () => {
    vi.spyOn(client, "getSource").mockResolvedValue(SOURCE);
    vi.spyOn(client, "getStatus").mockResolvedValue(STATUS);
    vi.spyOn(client, "getSourceConnectors").mockResolvedValue({
      connectors: [{ name: SOURCE_NAME, role: "source", kind: null, state: "RUNNING" }],
    });
    vi.spyOn(client, "getConnectorDlq").mockResolvedValue({
      has_dlq: true,
      topic: "s.dlq",
      count: 0,
      returned: 0,
      records: [],
    });

    renderDetail("ADMIN");

    fireEvent.click(await screen.findByRole("button", { name: /dead-letter|dlq/i }));
    expect(await screen.findByText(/no dropped records/i)).toBeInTheDocument();
  });

  it("shows the sink-DLQ hint on a connector without a DLQ", async () => {
    vi.spyOn(client, "getSource").mockResolvedValue(SOURCE);
    vi.spyOn(client, "getStatus").mockResolvedValue(STATUS);
    vi.spyOn(client, "getSourceConnectors").mockResolvedValue({
      connectors: [{ name: SOURCE_NAME, role: "source", kind: null, state: "RUNNING" }],
    });
    vi.spyOn(client, "getConnectorDlq").mockResolvedValue({
      has_dlq: false,
      hint: "dropped records for the pipeline land in the sink's DLQ",
    });

    renderDetail("ADMIN");

    fireEvent.click(await screen.findByRole("button", { name: /dead-letter|dlq/i }));
    expect(await screen.findByText(/sink's DLQ/i)).toBeInTheDocument();
  });

  it("renders the gitops remediation recipe when pause 409s", async () => {
    mockLoad();
    vi.spyOn(client, "pauseSource").mockRejectedValue(
      new client.ApiError(
        "409",
        409,
        JSON.stringify({
          detail: {
            message: "not supported",
            remediation: {
              reason: "r",
              where: "gitops",
              repo: "x@main",
              path: "pipelines/src",
              field: "spec.state",
              value: "paused",
              steps: ["edit", "commit"],
            },
          },
        }),
      ),
    );

    renderDetail("ADMIN");
    fireEvent.click(await screen.findByRole("button", { name: /^pause$/i }));
    expect(await screen.findByText(/spec\.state/)).toBeInTheDocument();
    expect(screen.getByText(/commit/)).toBeInTheDocument();
  });

  it("shows the ingestion/collector config for a kafka-ingest source", async () => {
    mockLoad();
    vi.spyOn(client, "getIngestConfig").mockResolvedValue({
      external_bootstrap: "kafka.example:9094",
      topic: "nginx-logs",
      disposition: "event",
      authoritative_fqn: "rawlake.nginx_raw.nginx_logs",
      producer: {
        user: "nginx-producer",
        mechanism: "SCRAM-SHA-512",
        password: "SECRET123",
        secret_ref: "nginx-producer",
      },
      expected_json: null,
      snippets: { fluentbit: "FLUENTBIT-SNIPPET", vector: "v", logstash: "l", generic: "g" },
    });

    renderDetail("ADMIN");

    fireEvent.click(
      await screen.findByRole("button", { name: /collector config|ingestion/i }),
    );

    expect(await screen.findByText(/FLUENTBIT-SNIPPET/)).toBeInTheDocument();
    expect(screen.getByText(/rawlake\.nginx_raw\.nginx_logs/)).toBeInTheDocument();
    expect(screen.queryByText("SECRET123")).toBeNull(); // hidden until revealed

    fireEvent.click(screen.getByRole("button", { name: /reveal|göster|show/i }));
    expect(await screen.findByText(/SECRET123/)).toBeInTheDocument();
  });

  it("updates credentials and confirms", async () => {
    mockLoad();
    const spy = vi
      .spyOn(client, "rotateCredentials")
      .mockResolvedValue({ ok: true, name: SOURCE_NAME, restarted: true });
    renderDetail("ADMIN");
    fireEvent.click(await screen.findByRole("button", { name: /update credentials/i }));
    fireEvent.change(screen.getByLabelText(/user/i), { target: { value: "u2" } });
    fireEvent.change(screen.getByLabelText(/password/i), { target: { value: "p2" } });
    fireEvent.click(screen.getByRole("button", { name: /save|update/i }));
    await waitFor(() =>
      expect(spy).toHaveBeenCalledWith(SOURCE_NAME, { user: "u2", password: "p2" }),
    );
    expect(await screen.findByText(/credentials updated|restarted/i)).toBeInTheDocument();
  });

  it("shows a note instead of the panel when getIngestConfig 400s (not a kafka-ingest source)", async () => {
    mockLoad();
    vi.spyOn(client, "getIngestConfig").mockRejectedValue(
      new client.ApiError("Bad Request", 400, "only available for kafka-ingest sources"),
    );

    renderDetail("ADMIN");

    fireEvent.click(
      await screen.findByRole("button", { name: /collector config|ingestion/i }),
    );

    expect(
      await screen.findByText(/only available for kafka-ingest sources/i),
    ).toBeInTheDocument();
  });

  // Snapshot lifecycle (Task 10): re-snapshot / progress / start-stop /
  // enable-snapshots, consuming Task 9's client functions.
  describe("snapshot lifecycle", () => {
    it("triggers an incremental snapshot with the entered tables and shows confirmation", async () => {
      mockLoad();
      const snapSpy = vi.spyOn(client, "triggerSnapshot").mockResolvedValue({
        ok: true,
        name: SOURCE_NAME,
        type: "incremental",
        data_collections: ["dbo.students"],
      });

      renderDetail("ADMIN");

      fireEvent.change(await screen.findByLabelText(/tables/i), {
        target: { value: "dbo.students" },
      });
      fireEvent.click(screen.getByRole("button", { name: /trigger snapshot/i }));

      await waitFor(() =>
        expect(snapSpy).toHaveBeenCalledWith(SOURCE_NAME, {
          type: "incremental",
          tables: ["dbo.students"],
        }),
      );
      expect(await screen.findByText(/snapshot triggered/i)).toBeInTheDocument();
    });

    it("shows the copyable signal-table DDL recipe when triggerSnapshot needs one", async () => {
      mockLoad();
      vi.spyOn(client, "triggerSnapshot").mockResolvedValue({
        needs_signal_table: true,
        dml: "CREATE TABLE dbo.debezium_signal (id VARCHAR(42) PRIMARY KEY, ...)",
      });
      const writeText = vi.fn();
      Object.assign(navigator, { clipboard: { writeText } });

      renderDetail("ADMIN");

      fireEvent.click(await screen.findByRole("button", { name: /trigger snapshot/i }));

      expect(
        await screen.findByText(/CREATE TABLE dbo\.debezium_signal/),
      ).toBeInTheDocument();
      expect(screen.getByText(/run this in your source db first/i)).toBeInTheDocument();
      // Not treated as an error.
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();

      fireEvent.click(screen.getByRole("button", { name: /^copy$/i }));
      expect(writeText).toHaveBeenCalledWith(
        "CREATE TABLE dbo.debezium_signal (id VARCHAR(42) PRIMARY KEY, ...)",
      );
      expect(await screen.findByText(/copied/i)).toBeInTheDocument();
    });

    it("triggers a blocking snapshot with no tables when Blocking is selected and the input is left blank", async () => {
      mockLoad();
      const snapSpy = vi
        .spyOn(client, "triggerSnapshot")
        .mockResolvedValue({ ok: true, name: SOURCE_NAME, type: "blocking" });

      renderDetail("ADMIN");

      fireEvent.click(await screen.findByLabelText(/^blocking$/i));
      fireEvent.click(screen.getByRole("button", { name: /trigger snapshot/i }));

      await waitFor(() =>
        expect(snapSpy).toHaveBeenCalledWith(SOURCE_NAME, {
          type: "blocking",
          tables: undefined,
        }),
      );
    });

    it("fetches and renders snapshot progress on demand, with no polling", async () => {
      mockLoad();
      const progressSpy = vi.spyOn(client, "getSnapshotProgress").mockResolvedValue({
        notifications: [
          { kind: "in_progress", table: "dbo.students", progress: "500/1000", ts: 1730000000000 },
        ],
      });

      renderDetail("ADMIN");

      fireEvent.click(
        await screen.findByRole("button", { name: /refresh snapshot progress/i }),
      );

      await waitFor(() => expect(progressSpy).toHaveBeenCalledWith(SOURCE_NAME));
      expect(progressSpy).toHaveBeenCalledTimes(1);
      expect(await screen.findByText("dbo.students")).toBeInTheDocument();
      expect(screen.getByText("500/1000")).toBeInTheDocument();
    });

    it("shows a no-activity note when snapshot progress returns no notifications", async () => {
      mockLoad();
      vi.spyOn(client, "getSnapshotProgress").mockResolvedValue({ notifications: [] });

      renderDetail("ADMIN");

      fireEvent.click(
        await screen.findByRole("button", { name: /refresh snapshot progress/i }),
      );

      expect(await screen.findByText(/no recent snapshot activity/i)).toBeInTheDocument();
    });

    it("starts and stops a source", async () => {
      mockLoad();
      const startSpy = vi
        .spyOn(client, "startSource")
        .mockResolvedValue({ ok: true, name: SOURCE_NAME, stopped: false });
      const stopSpy = vi
        .spyOn(client, "stopSource")
        .mockResolvedValue({ ok: true, name: SOURCE_NAME, stopped: true });

      renderDetail("ADMIN");

      fireEvent.click(await screen.findByRole("button", { name: /^start$/i }));
      await waitFor(() => expect(startSpy).toHaveBeenCalledWith(SOURCE_NAME));

      fireEvent.click(screen.getByRole("button", { name: /^stop$/i }));
      await waitFor(() => expect(stopSpy).toHaveBeenCalledWith(SOURCE_NAME));
    });

    it("renders the gitops remediation recipe when start 409s (shared remediation renderer)", async () => {
      mockLoad();
      vi.spyOn(client, "startSource").mockRejectedValue(
        new client.ApiError(
          "409",
          409,
          JSON.stringify({
            detail: {
              message: "not supported",
              remediation: {
                reason: "r",
                where: "gitops",
                repo: "x@main",
                path: "pipelines/src",
                field: "spec.state",
                value: "running",
                steps: ["edit", "commit"],
              },
            },
          }),
        ),
      );

      renderDetail("ADMIN");
      fireEvent.click(await screen.findByRole("button", { name: /^start$/i }));
      expect(await screen.findByText(/spec\.state/)).toBeInTheDocument();
    });

    it("enables snapshot signaling on an existing connector", async () => {
      mockLoad();
      const spy = vi
        .spyOn(client, "enableSnapshots")
        .mockResolvedValue({ ok: true, name: SOURCE_NAME, snapshots_enabled: true });

      renderDetail("ADMIN");
      fireEvent.click(await screen.findByRole("button", { name: /enable snapshots/i }));

      await waitFor(() => expect(spy).toHaveBeenCalledWith(SOURCE_NAME));
      expect(await screen.findByText(/snapshot signaling enabled/i)).toBeInTheDocument();
    });

    it("hides re-snapshot/progress/enable-snapshots for a spark-batch source", async () => {
      vi.spyOn(client, "getSource").mockResolvedValue(SPARK_SOURCE);
      vi.spyOn(client, "getStatus").mockResolvedValue({ connectors: [], reachable: true });
      vi.spyOn(client, "getSourceConnectors").mockResolvedValue({ connectors: [] });

      renderDetail("ADMIN", SPARK_SOURCE_NAME);

      expect(await screen.findByText(SPARK_SOURCE_NAME)).toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: /trigger snapshot/i }),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: /refresh snapshot progress/i }),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: /enable snapshots/i }),
      ).not.toBeInTheDocument();
    });

    it("hides re-snapshot/progress/enable-snapshots for a kafka-ingest source, but still shows Start/Stop", async () => {
      vi.spyOn(client, "getSource").mockResolvedValue(KAFKA_INGEST_SOURCE);
      vi.spyOn(client, "getStatus").mockResolvedValue({ connectors: [], reachable: true });
      vi.spyOn(client, "getSourceConnectors").mockResolvedValue({ connectors: [] });

      renderDetail("ADMIN", KAFKA_INGEST_SOURCE.name);

      expect(await screen.findByText(KAFKA_INGEST_SOURCE.name)).toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: /trigger snapshot/i }),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: /enable snapshots/i }),
      ).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: /^start$/i })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /^stop$/i })).toBeInTheDocument();
    });
  });
});
