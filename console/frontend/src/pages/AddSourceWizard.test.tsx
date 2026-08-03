import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import type { SourceTypeDescriptor } from "../api/client";
import AddSourceWizard from "./AddSourceWizard";

// Mirrors the live registry (console/backend/app/source_types.py) as of
// Plan B1 Task 4 -- kept in sync so the wizard's default-path tests exercise
// the same shape `GET /api/sources/types` really returns.
const DEFAULT_SOURCE_TYPES: SourceTypeDescriptor[] = [
  {
    id: "cdc-mssql", kind: "cdc", type: "mssql", lane: "debezium-cdc",
    disposition: "entity", dispositions: ["entity"],
    required_fields: ["db_host"], needs_bootstrap: false,
  },
  {
    id: "cdc-pg", kind: "cdc", type: "pg", lane: "debezium-cdc",
    disposition: "entity", dispositions: ["entity"],
    required_fields: ["db_host"], needs_bootstrap: false,
  },
  {
    id: "cdc-mongo", kind: "cdc", type: "mongo", lane: "debezium-cdc",
    disposition: "entity", dispositions: ["entity"],
    required_fields: ["mongo_uri"], needs_bootstrap: false,
  },
  {
    id: "cdc-mysql", kind: "cdc", type: "mysql", lane: "debezium-cdc",
    disposition: "entity", dispositions: ["entity"],
    required_fields: ["db_host"], needs_bootstrap: false,
  },
  {
    id: "scheduled-jdbc-mssql", kind: "scheduled", type: "mssql", lane: "kafka-connect-source",
    disposition: "entity", dispositions: ["entity"],
    required_fields: ["jdbc_url", "incrementing_col"], needs_bootstrap: false,
  },
  {
    id: "scheduled-jdbc-pg", kind: "scheduled", type: "pg", lane: "kafka-connect-source",
    disposition: "entity", dispositions: ["entity"],
    required_fields: ["jdbc_url", "incrementing_col"], needs_bootstrap: false,
  },
  {
    id: "scheduled-mongo", kind: "scheduled", type: "mongo", lane: "spark-batch",
    disposition: "entity", dispositions: ["entity"],
    required_fields: ["cron"], needs_bootstrap: false,
  },
  {
    id: "stream-kafka", kind: "stream", type: "kafka", lane: "kafka-connect-source",
    disposition: "event", dispositions: ["event", "entity"],
    required_fields: [], needs_bootstrap: true,
  },
  {
    id: "stream-http", kind: "stream", type: "http", lane: "kafka-connect-source",
    disposition: "event", dispositions: ["event", "entity"],
    required_fields: ["http_url"], needs_bootstrap: false,
  },
  {
    id: "stream-mqtt", kind: "stream", type: "mqtt", lane: "kafka-connect-source",
    disposition: "event", dispositions: ["event"],
    required_fields: ["mqtt_broker", "mqtt_topic"], needs_bootstrap: false,
  },
  {
    id: "stream-rabbitmq", kind: "stream", type: "rabbitmq", lane: "kafka-connect-source",
    disposition: "event", dispositions: ["event"],
    required_fields: ["rabbitmq_uri", "rabbitmq_queue"], needs_bootstrap: false,
  },
];

beforeEach(() => {
  vi.spyOn(client, "getSourceTypes").mockResolvedValue(DEFAULT_SOURCE_TYPES);
});

const PREVIEW_RESPONSE = {
  bronze_bucket: "bronze-mssql_ogrenci",
  silver_bucket: "silver-mssql_ogrenci",
  namespace_ddl:
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.mssql_ogrenci WITH (location='s3://silver-mssql_ogrenci/warehouse')",
  connector: {
    apiVersion: "kafka.strimzi.io/v1beta2",
    kind: "KafkaConnector",
    metadata: { name: "dbz-mssql1-students", namespace: "example" },
    spec: {
      class: "io.debezium.connector.sqlserver.SqlServerConnector",
      config: { "transforms.route.static.value": "mssql_ogrenci.students" },
    },
  },
  kafka_topic: {
    apiVersion: "kafka.strimzi.io/v1beta2",
    kind: "KafkaTopic",
    metadata: { name: "cdc.mssql1.dbo.students", namespace: "example" },
    spec: { partitions: 6, replicas: 3 },
  },
};

function fillStep1() {
  // Defaults (kind=cdc, type=mssql) are already what we want -- just advance.
  fireEvent.click(screen.getByRole("button", { name: /next/i }));
}

async function fillStep2() {
  fireEvent.change(screen.getByLabelText(/source name/i), {
    target: { value: "mssql1" },
  });
  // db_host only renders once the registry fetch (kicked off on mount)
  // resolves and the "cdc"+"mssql" descriptor is known to require it --
  // findByLabelText polls, giving that microtask a chance to land.
  const dbHost = await screen.findByLabelText(/database host/i);
  fireEvent.change(dbHost, {
    target: { value: "mssql1.internal" },
  });
  fireEvent.change(screen.getByLabelText(/^username/i), {
    target: { value: "sa" },
  });
  fireEvent.change(screen.getByLabelText(/^password/i), {
    target: { value: "s3cret" },
  });
  fireEvent.click(screen.getByRole("button", { name: /next/i }));
}

function fillStep3() {
  fireEvent.change(screen.getByLabelText(/^database$/i), {
    target: { value: "school" },
  });
  fireEvent.change(screen.getByLabelText(/^table$/i), {
    target: { value: "dbo.students" },
  });
  fireEvent.click(screen.getByRole("button", { name: /next/i }));
}

function fillStep4() {
  fireEvent.change(screen.getByLabelText(/pipeline name/i), {
    target: { value: "mssql_ogrenci" },
  });
  fireEvent.change(screen.getByLabelText(/target table/i), {
    target: { value: "students" },
  });
  fireEvent.click(screen.getByRole("button", { name: /next/i }));
}

async function fillThroughPreview() {
  fillStep1();
  await fillStep2();
  fillStep3();
  fillStep4();
  // Now on the preview step.
  fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));
  await waitFor(() =>
    expect(
      screen.getByText("io.debezium.connector.sqlserver.SqlServerConnector"),
    ).toBeInTheDocument(),
  );
}

describe("AddSourceWizard", () => {
  it("walks through every step, previews the rendered CRs, then submits", async () => {
    const previewSpy = vi
      .spyOn(client, "previewSource")
      .mockResolvedValue(PREVIEW_RESPONSE);
    const createSpy = vi
      .spyOn(client, "createSource")
      .mockResolvedValue({ ok: true, steps: [] });

    render(<AddSourceWizard />);

    await fillThroughPreview();

    // Preview shows connector class, topic name, both per-pipeline buckets,
    // and DDL.
    expect(
      screen.getByText("io.debezium.connector.sqlserver.SqlServerConnector"),
    ).toBeInTheDocument();
    expect(screen.getByText("cdc.mssql1.dbo.students")).toBeInTheDocument();
    expect(screen.getByText("bronze-mssql_ogrenci")).toBeInTheDocument();
    expect(screen.getByText("silver-mssql_ogrenci")).toBeInTheDocument();
    expect(
      screen.getByText(/CREATE NAMESPACE IF NOT EXISTS lakehouse\.mssql_ogrenci/),
    ).toBeInTheDocument();

    // previewSource was called with no credentials -- just the assembled spec.
    expect(previewSpy).toHaveBeenCalledTimes(1);
    expect(previewSpy).toHaveBeenCalledWith({
      source: "mssql1",
      kind: "cdc",
      type: "mssql",
      db: "school",
      table: "dbo.students",
      target_ns: "mssql_ogrenci",
      target_table: "students",
      db_host: "mssql1.internal",
      // CDC lanes always send an explicit snapshot_mode (Task 9) -- "initial"
      // is the wizard's stated default.
      snapshot_mode: "initial",
    });

    // Advance to the final "submit" step and create the source.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /create source/i }));

    await waitFor(() => expect(createSpy).toHaveBeenCalledTimes(1));
    expect(createSpy).toHaveBeenCalledWith(
      {
        source: "mssql1",
        kind: "cdc",
        type: "mssql",
        db: "school",
        table: "dbo.students",
        target_ns: "mssql_ogrenci",
        target_table: "students",
        db_host: "mssql1.internal",
        snapshot_mode: "initial",
      },
      { user: "sa", password: "s3cret" },
    );

    expect(await screen.findByText(/source created/i)).toBeInTheDocument();
  });

  it("labels the target_ns field \"Pipeline name\" (per-pipeline unique namespace, B-v2)", async () => {
    render(<AddSourceWizard />);
    fillStep1();
    await fillStep2();
    fillStep3();

    expect(screen.getByLabelText(/^pipeline name$/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/target namespace/i)).not.toBeInTheDocument();
  });

  it("auto-fills the pipeline name from source+target_table, but stays editable", async () => {
    render(<AddSourceWizard />);
    fillStep1();
    await fillStep2(); // sets source to "mssql1"
    fillStep3();

    const pipelineName = screen.getByLabelText(/^pipeline name$/i) as HTMLInputElement;
    // Neither field has a target_table yet -- stays empty.
    expect(pipelineName.value).toBe("");

    fireEvent.change(screen.getByLabelText(/target table/i), {
      target: { value: "dbo.students" },
    });
    // Auto-fills to a sanitized "<source>_<target_table>": lowercased, with
    // the "." (and any other non-alnum run) collapsed to "_".
    expect(pipelineName.value).toBe("mssql1_dbo_students");

    // The field stays editable -- an explicit edit overrides the default...
    fireEvent.change(pipelineName, { target: { value: "custom_pipeline" } });
    expect(pipelineName.value).toBe("custom_pipeline");

    // ...and a later target_table edit must not clobber that override.
    fireEvent.change(screen.getByLabelText(/target table/i), {
      target: { value: "dbo.other" },
    });
    expect(pipelineName.value).toBe("custom_pipeline");
  });

  it("shows a failure state (not success) when createSource resolves with ok:false", async () => {
    // The backend can return a 2xx-range response (201 or 207) whose body
    // still says `ok: false` -- an in-band pipeline failure (rollback ran,
    // or unsupported scheduled+mongo). The wizard must not report success
    // just because the promise resolved; it has to inspect `result.ok`.
    vi.spyOn(client, "previewSource").mockResolvedValue(PREVIEW_RESPONSE);
    vi.spyOn(client, "createSource").mockResolvedValue({
      ok: false,
      steps: [
        { name: "secret", ok: true, detail: "" },
        { name: "verify", ok: false, detail: "connector dbz-mssql1-students reported FAILED state" },
      ],
    });

    render(<AddSourceWizard />);

    await fillThroughPreview();
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /create source/i }));

    expect(await screen.findByText(/source creation failed/i)).toBeInTheDocument();
    expect(
      screen.getByText(/verify: connector dbz-mssql1-students reported FAILED state/i),
    ).toBeInTheDocument();
    expect(screen.queryByText(/^source created\.?$/i)).not.toBeInTheDocument();
  });

  it("shows an error message when preview fails, without submitting anything", async () => {
    vi.spyOn(client, "previewSource").mockRejectedValue(new Error("boom"));
    const createSpy = vi.spyOn(client, "createSource");

    render(<AddSourceWizard />);

    fillStep1();
    await fillStep2();
    fillStep3();
    fillStep4();
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    expect(await screen.findByText(/boom/i)).toBeInTheDocument();
    expect(createSpy).not.toHaveBeenCalled();
  });

  it("lists stream/kafka from the registry and shows a bootstrap field", async () => {
    vi.spyOn(client, "getSourceTypes").mockResolvedValue([
      {
        id: "cdc-pg", kind: "cdc", type: "pg", lane: "debezium-cdc", disposition: "entity",
        dispositions: ["entity"], required_fields: ["db_host"], needs_bootstrap: false,
      },
      {
        id: "stream-kafka", kind: "stream", type: "kafka", lane: "kafka-connect-source",
        disposition: "event", dispositions: ["event"], required_fields: [], needs_bootstrap: true,
      },
    ]);

    render(<AddSourceWizard />);

    // Type options come from the mocked registry, not a hard-coded
    // mssql/scheduled/mongo list -- "scheduled" and "mssql" (the wizard's
    // former hard-coded defaults) are absent from this two-entry registry.
    await screen.findByRole("option", { name: /^stream$/i });
    expect(screen.queryByRole("option", { name: /^scheduled$/i })).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    expect(screen.queryByRole("option", { name: /^mssql$/i })).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });

    // Choosing the needs_bootstrap=true type surfaces the bootstrap input.
    expect(
      await screen.findByLabelText(/kafka bootstrap \(external, optional\)/i),
    ).toBeInTheDocument();
  });

  it("shows s3 bucket/prefix/format fields for batch/s3", async () => {
    vi.spyOn(client, "getSourceTypes").mockResolvedValue([
      {
        id: "batch-s3", kind: "batch", type: "s3", lane: "spark-batch", disposition: "entity",
        dispositions: ["entity"], required_fields: ["s3_bucket", "s3_prefix", "file_format", "cron"],
        needs_bootstrap: false,
      },
    ]);

    render(<AddSourceWizard />);

    // Registry has only batch/s3 -- kind/type default to it once loaded.
    await screen.findByRole("option", { name: /^batch$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "batch" } });
    await screen.findByRole("option", { name: /^s3$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "s3" } });

    // Advance to step 2 -- s3_bucket/s3_prefix/file_format render there.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));

    expect(await screen.findByLabelText(/s3 bucket/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/s3 prefix/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/file format/i)).toBeInTheDocument();
    // parquet/json/avro options on the format select.
    expect(screen.getByRole("option", { name: /^parquet$/i })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: /^json$/i })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: /^avro$/i })).toBeInTheDocument();

    // Advance to step 3 -- cron renders there (required_fields-driven, not
    // hard-coded off kind === "scheduled").
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    expect(screen.getByLabelText(/cron schedule/i)).toBeInTheDocument();
  });

  it("shows an error alert (and doesn't crash) when getSourceTypes fails on mount", async () => {
    vi.spyOn(client, "getSourceTypes").mockRejectedValue(new Error("registry unreachable"));

    render(<AddSourceWizard />);

    expect(await screen.findByRole("alert")).toHaveTextContent(
      /failed to load source types: registry unreachable/i,
    );
  });

  it("shows broker + topic fields for stream/mqtt", async () => {
    vi.spyOn(client, "getSourceTypes").mockResolvedValue([
      { id: "stream-mqtt", kind: "stream", type: "mqtt", lane: "kafka-connect-source", disposition: "event",
        dispositions: ["event"], required_fields: ["mqtt_broker", "mqtt_topic"], needs_bootstrap: false },
    ]);
    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^mqtt$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "mqtt" } });
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    expect(await screen.findByLabelText(/mqtt broker/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/mqtt topic/i)).toBeInTheDocument();
  });

  it("shows a URL field and event/entity disposition for stream/http", async () => {
    vi.spyOn(client, "getSourceTypes").mockResolvedValue([
      { id: "stream-http", kind: "stream", type: "http", lane: "kafka-connect-source", disposition: "event",
        dispositions: ["event", "entity"], required_fields: ["http_url"], needs_bootstrap: false },
    ]);
    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^http$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "http" } });
    // stream-http has 2 dispositions -> selector renders on step 1
    expect(await screen.findByLabelText(/disposition/i)).toBeInTheDocument();
    expect(screen.getByRole("option", { name: /^entity$/i })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    expect(await screen.findByLabelText(/http url/i)).toBeInTheDocument();
  });

  it("shows columns/primary key/delete_field for stream/kafka + entity", async () => {
    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });

    // stream/kafka reads its 2-way disposition as an explicit event/entity
    // fork (Task 8), not the generic <select> other multi-disposition types
    // still use -- both write into the same `form.disposition` state.
    fireEvent.click(await screen.findByLabelText(/entity/i));

    expect(screen.getByLabelText(/columns/i)).toBeInTheDocument();
    // Task 9: the old "Identifier" input is relabeled "Primary key column(s)"
    // -- now the PRIMARY, required-for-entity path; columns above is optional/
    // advanced.
    expect(screen.getByLabelText(/primary key/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/delete field/i)).toBeInTheDocument();
  });

  it("hides columns/identifier/delete_field for stream/kafka + event (default)", async () => {
    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });

    // Default disposition (unselected) resolves to "event" -- entity-only
    // fields must stay hidden.
    await screen.findByLabelText(/log\/event/i);
    expect(screen.queryByLabelText(/columns/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/identifier/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/delete field/i)).not.toBeInTheDocument();
  });

  it("wires filled columns/primary key/delete_field into the built spec for stream/kafka + entity", async () => {
    const previewSpy = vi
      .spyOn(client, "previewSource")
      .mockResolvedValue(PREVIEW_RESPONSE);

    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });
    fireEvent.click(await screen.findByLabelText(/entity/i));

    fireEvent.change(screen.getByLabelText(/columns/i), {
      target: { value: "id:bigint,name:varchar" },
    });
    fireEvent.change(screen.getByLabelText(/primary key/i), {
      target: { value: "id" },
    });
    fireEvent.change(screen.getByLabelText(/delete field/i), {
      target: { value: "_deleted" },
    });

    // Step 1 -> 2: source name.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.change(screen.getByLabelText(/source name/i), {
      target: { value: "kafka1" },
    });
    // Step 2 -> 3 -> 4: no required fields for stream/kafka; fill the target.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.change(screen.getByLabelText(/pipeline name/i), {
      target: { value: "ns1" },
    });
    fireEvent.change(screen.getByLabelText(/target table/i), {
      target: { value: "tbl1" },
    });
    // Step 4 -> 5: fetch preview, which calls buildSpec under the hood.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    await waitFor(() => expect(previewSpy).toHaveBeenCalledTimes(1));
    const [spec] = previewSpy.mock.calls[0];
    expect(spec.columns).toEqual([
      { name: "id", type: "bigint" },
      { name: "name", type: "varchar" },
    ]);
    expect(spec.identifier).toEqual(["id"]);
    expect(spec.delete_field).toBe("_deleted");
  });

  it("shows the event/entity fork for stream-kafka and toggles PK fields", async () => {
    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });

    const eventRadio = await screen.findByLabelText(/log\/event/i);
    const entityRadio = screen.getByLabelText(/entity/i);
    fireEvent.click(eventRadio);
    expect(screen.queryByLabelText(/identifier|primary key|columns/i)).toBeNull();
    fireEvent.click(entityRadio);
    expect(screen.getAllByLabelText(/columns|identifier/i).length).toBeGreaterThan(0);
  });

  it("offers a create-topic option for stream-kafka", async () => {
    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });

    expect(await screen.findByLabelText(/create.*topic/i)).toBeInTheDocument();
  });

  it("shows an expected-JSON preview for stream/kafka + entity, marking the PK", async () => {
    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });
    fireEvent.click(await screen.findByLabelText(/entity/i));

    fireEvent.change(screen.getByLabelText(/columns/i), {
      target: { value: "id:bigint,name:varchar" },
    });
    fireEvent.change(screen.getByLabelText(/primary key/i), {
      target: { value: "id" },
    });

    expect(screen.getByText(/"id": "bigint \(PK\)"/)).toBeInTheDocument();
    expect(screen.getByText(/"name": "varchar"/)).toBeInTheDocument();
  });

  it("fetches and renders the ingest config panel after a successful stream/kafka create", async () => {
    vi.spyOn(client, "previewSource").mockResolvedValue(PREVIEW_RESPONSE);
    vi.spyOn(client, "createSource").mockResolvedValue({
      ok: true,
      steps: [],
      connector_name: "kafka-ingest-k1-orders",
    });
    const ingestConfigSpy = vi.spyOn(client, "getIngestConfig").mockResolvedValue({
      external_bootstrap: "kafka.example:9092",
      topic: "kafka1.raw",
      disposition: "event",
      authoritative_fqn: "lakehouse.kafka1.raw",
      producer: {
        user: "kafka1-producer",
        mechanism: "SCRAM-SHA-512",
        password: "s3cr3t-pass",
        secret_ref: "kafka1-producer-credentials",
      },
      expected_json: null,
      snippets: {
        fluentbit: "fluentbit config for kafka1",
        vector: "vector config for kafka1",
        logstash: "logstash config for kafka1",
        generic: "generic config for kafka1",
      },
    });

    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });

    // Step 1 -> 2: source name.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.change(screen.getByLabelText(/source name/i), {
      target: { value: "kafka1" },
    });
    // Step 2 -> 3 -> 4: no required fields for stream/kafka; fill the target.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.change(screen.getByLabelText(/pipeline name/i), {
      target: { value: "ns1" },
    });
    fireEvent.change(screen.getByLabelText(/target table/i), {
      target: { value: "tbl1" },
    });
    // Step 4 -> 5: fetch preview.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));
    await waitFor(() =>
      expect(
        screen.getByText("io.debezium.connector.sqlserver.SqlServerConnector"),
      ).toBeInTheDocument(),
    );

    // Step 5 -> 6: create the source.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /create source/i }));

    expect(await screen.findByText(/source created/i)).toBeInTheDocument();
    // Residual fix: fetch ingestion config by the CREATED connector's
    // composite name (returned by createSource), not the bare source id --
    // the bare id is ambiguous when one source id owns multiple kafka-ingest
    // target tables.
    await waitFor(() => expect(ingestConfigSpy).toHaveBeenCalledWith("kafka-ingest-k1-orders"));
    expect(await screen.findByText("kafka1.raw")).toBeInTheDocument();
    // Credential stays hidden until the reveal toggle is clicked.
    expect(screen.queryByText("s3cr3t-pass")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /reveal/i }));
    expect(await screen.findByText("s3cr3t-pass")).toBeInTheDocument();
  });

  it("falls back to the bare source id when createSource omits connector_name", async () => {
    // Safety net for older/partial responses (e.g. gitops/spark-batch lanes,
    // or a backend that hasn't been upgraded yet): if connector_name is
    // absent, the wizard must preserve its previous behavior of fetching by
    // the bare source id.
    vi.spyOn(client, "previewSource").mockResolvedValue(PREVIEW_RESPONSE);
    vi.spyOn(client, "createSource").mockResolvedValue({ ok: true, steps: [] });
    const ingestConfigSpy = vi.spyOn(client, "getIngestConfig").mockResolvedValue({
      external_bootstrap: "kafka.example:9092",
      topic: "kafka1.raw",
      disposition: "event",
      authoritative_fqn: "lakehouse.kafka1.raw",
      producer: {
        user: "kafka1-producer",
        mechanism: "SCRAM-SHA-512",
        password: "s3cr3t-pass",
        secret_ref: "kafka1-producer-credentials",
      },
      expected_json: null,
      snippets: {
        fluentbit: "fluentbit config for kafka1",
        vector: "vector config for kafka1",
        logstash: "logstash config for kafka1",
        generic: "generic config for kafka1",
      },
    });

    render(<AddSourceWizard />);
    await screen.findByRole("option", { name: /^stream$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "stream" } });
    await screen.findByRole("option", { name: /^kafka$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "kafka" } });

    // Step 1 -> 2: source name.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.change(screen.getByLabelText(/source name/i), {
      target: { value: "kafka1" },
    });
    // Step 2 -> 3 -> 4: no required fields for stream/kafka; fill the target.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.change(screen.getByLabelText(/pipeline name/i), {
      target: { value: "ns1" },
    });
    fireEvent.change(screen.getByLabelText(/target table/i), {
      target: { value: "tbl1" },
    });
    // Step 4 -> 5: fetch preview.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));
    await waitFor(() =>
      expect(
        screen.getByText("io.debezium.connector.sqlserver.SqlServerConnector"),
      ).toBeInTheDocument(),
    );

    // Step 5 -> 6: create the source.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /create source/i }));

    expect(await screen.findByText(/source created/i)).toBeInTheDocument();
    await waitFor(() => expect(ingestConfigSpy).toHaveBeenCalledWith("kafka1"));
  });

  // --- Task 9: Test Connection + snapshot-mode selector + PK-only input ---

  it("tests the connection on step 2 (advisory, connector/DB lane) and shows the result", async () => {
    const spy = vi
      .spyOn(client, "testConnection")
      .mockResolvedValue({ applicable: true, ok: true, errors: [] });

    render(<AddSourceWizard />);
    fillStep1(); // defaults (cdc/mssql) land on step 2
    fireEvent.change(screen.getByLabelText(/source name/i), {
      target: { value: "mssql1" },
    });
    const dbHost = await screen.findByLabelText(/database host/i);
    fireEvent.change(dbHost, { target: { value: "mssql1.internal" } });
    fireEvent.change(screen.getByLabelText(/^username/i), { target: { value: "sa" } });
    fireEvent.change(screen.getByLabelText(/^password/i), { target: { value: "s3cret" } });

    fireEvent.click(screen.getByRole("button", { name: /test connection/i }));
    await waitFor(() => expect(spy).toHaveBeenCalledTimes(1));
    expect(spy).toHaveBeenCalledWith(
      expect.objectContaining({ source: "mssql1", db_host: "mssql1.internal" }),
      { user: "sa", password: "s3cret" },
    );
    expect(await screen.findByText(/connection ok/i)).toBeInTheDocument();

    // Advisory only -- never blocks Next, whatever the result.
    expect(screen.getByRole("button", { name: /^next$/i })).not.toBeDisabled();
  });

  it("shows the mapped error list when test-connection reports ok:false", async () => {
    vi.spyOn(client, "testConnection").mockResolvedValue({
      applicable: true,
      ok: false,
      errors: [{ field: "db_host", message: "could not resolve host" }],
    });

    render(<AddSourceWizard />);
    fillStep1();
    fireEvent.change(screen.getByLabelText(/source name/i), {
      target: { value: "mssql1" },
    });
    await screen.findByLabelText(/database host/i);

    fireEvent.click(screen.getByRole("button", { name: /test connection/i }));
    expect(await screen.findByText(/could not resolve host/i)).toBeInTheDocument();
    expect(screen.queryByText(/connection ok/i)).not.toBeInTheDocument();
  });

  it("offers an initial-snapshot-mode selector for CDC lanes, defaulting to initial", async () => {
    render(<AddSourceWizard />);

    // Anchored to the select's own label ("Initial snapshot") -- Task 11's
    // signaling-table field also mentions "snapshots" in its own label, so a
    // loose /snapshot/i match is now ambiguous between the two fields.
    const snapshotSelect = (await screen.findByLabelText(
      /^initial snapshot$/i,
    )) as HTMLSelectElement;
    expect(snapshotSelect).toBeInTheDocument();
    expect(snapshotSelect.value).toBe("initial");
    expect(screen.getByText(/resumable incremental snapshot/i)).toBeInTheDocument();

    fireEvent.change(snapshotSelect, { target: { value: "no_data" } });
    expect(snapshotSelect.value).toBe("no_data");
  });

  it("asks only for the primary key on an entity source (CDC default disposition)", async () => {
    render(<AddSourceWizard />);
    expect(await screen.findByLabelText(/primary key/i)).toBeInTheDocument();
  });

  it("wires the primary-key + snapshot-mode fields into the built spec for CDC", async () => {
    const previewSpy = vi
      .spyOn(client, "previewSource")
      .mockResolvedValue(PREVIEW_RESPONSE);

    render(<AddSourceWizard />);
    fireEvent.change(await screen.findByLabelText(/primary key/i), {
      target: { value: "id:bigint" },
    });
    fireEvent.change(screen.getByLabelText(/^initial snapshot$/i), {
      target: { value: "no_data" },
    });

    fillStep1(); // advance from step 1 (where the PK/snapshot fields live) to step 2
    await fillStep2();
    fillStep3();
    fillStep4();
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    await waitFor(() => expect(previewSpy).toHaveBeenCalledTimes(1));
    const [spec] = previewSpy.mock.calls[0];
    expect(spec.identifier).toEqual(["id"]);
    expect(spec.columns).toEqual([{ name: "id", type: "bigint" }]);
    expect(spec.snapshot_mode).toBe("no_data");
  });

  // --- Task 11: create-stopped + signaling-table fields ---

  it("shows a create-stopped checkbox and signaling-table field for CDC lanes", async () => {
    render(<AddSourceWizard />);
    expect(await screen.findByLabelText(/create stopped/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/signaling table/i)).toBeInTheDocument();
    expect(screen.getByText(/create this table in your source db/i)).toBeInTheDocument();
  });

  it("hides the create-stopped/signaling-table fields for non-CDC lanes", async () => {
    render(<AddSourceWizard />);
    // Defaults (cdc/mssql) show the fields first...
    await screen.findByLabelText(/create stopped/i);

    await screen.findByRole("option", { name: /^scheduled$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "scheduled" } });

    expect(screen.queryByLabelText(/create stopped/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/signaling table/i)).not.toBeInTheDocument();
  });

  it("wires create-stopped + signaling-table into the built spec for CDC", async () => {
    const previewSpy = vi
      .spyOn(client, "previewSource")
      .mockResolvedValue(PREVIEW_RESPONSE);

    render(<AddSourceWizard />);
    fireEvent.click(await screen.findByLabelText(/create stopped/i));
    fireEvent.change(screen.getByLabelText(/signaling table/i), {
      target: { value: "public.debezium_signal" },
    });

    fillStep1(); // advance from step 1 (where the new fields live) to step 2
    await fillStep2();
    fillStep3();
    fillStep4();
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    await waitFor(() => expect(previewSpy).toHaveBeenCalledTimes(1));
    const [spec] = previewSpy.mock.calls[0];
    expect(spec.create_stopped).toBe(true);
    expect(spec.signal_data_collection).toBe("public.debezium_signal");
  });

  it("omits create_stopped/signal_data_collection from the built spec when left at their defaults", async () => {
    const previewSpy = vi
      .spyOn(client, "previewSource")
      .mockResolvedValue(PREVIEW_RESPONSE);

    render(<AddSourceWizard />);
    fillStep1();
    await fillStep2();
    fillStep3();
    fillStep4();
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    await waitFor(() => expect(previewSpy).toHaveBeenCalledTimes(1));
    const [spec] = previewSpy.mock.calls[0];
    expect(spec.create_stopped).toBeUndefined();
    expect(spec.signal_data_collection).toBeUndefined();
  });

  // --- Silver scale-hardening Task 8: write-mode selector + bucket-count ---

  it("shows a Silver write-mode selector (defaulting to COW) and bucket-count field for CDC lanes", async () => {
    render(<AddSourceWizard />);

    const writeMode = (await screen.findByLabelText(
      /silver write mode/i,
    )) as HTMLSelectElement;
    expect(writeMode.value).toBe("");
    expect(screen.getByRole("option", { name: /copy-on-write/i })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: /merge-on-read/i })).toBeInTheDocument();
    expect(
      screen.getByText(/yalnızca-ekleme \(append-only\) veya seyrek güncellenen/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/yüksek-frekanslı güncellemeler ve yazma-yoğun görevler için mor/i),
    ).toBeInTheDocument();

    const bucketCount = screen.getByLabelText(/silver bucket count/i) as HTMLInputElement;
    expect(bucketCount.value).toBe("");
    expect(bucketCount.placeholder).toBe("16");
    expect(
      screen.getByText(/varsayılan 16; çok büyük tablolar \(10m\+ satır\)/i),
    ).toBeInTheDocument();
  });

  it("hides the Silver write-mode/bucket-count fields for non-CDC lanes", async () => {
    render(<AddSourceWizard />);
    // Defaults (cdc/mssql) show the fields first...
    await screen.findByLabelText(/silver write mode/i);

    await screen.findByRole("option", { name: /^scheduled$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "scheduled" } });

    expect(screen.queryByLabelText(/silver write mode/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/silver bucket count/i)).not.toBeInTheDocument();
  });

  it("wires the Silver write-mode + bucket-count into the built spec for CDC", async () => {
    const previewSpy = vi
      .spyOn(client, "previewSource")
      .mockResolvedValue(PREVIEW_RESPONSE);

    render(<AddSourceWizard />);
    fireEvent.change(await screen.findByLabelText(/silver write mode/i), {
      target: { value: "merge-on-read" },
    });
    fireEvent.change(screen.getByLabelText(/silver bucket count/i), {
      target: { value: "64" },
    });

    fillStep1(); // advance from step 1 (where the new fields live) to step 2
    await fillStep2();
    fillStep3();
    fillStep4();
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    await waitFor(() => expect(previewSpy).toHaveBeenCalledTimes(1));
    const [spec] = previewSpy.mock.calls[0];
    expect(spec.silver_write_mode).toBe("merge-on-read");
    expect(spec.silver_bucket_count).toBe(64);
  });

  it("omits silver_write_mode/silver_bucket_count from the built spec when left at their defaults", async () => {
    const previewSpy = vi
      .spyOn(client, "previewSource")
      .mockResolvedValue(PREVIEW_RESPONSE);

    render(<AddSourceWizard />);
    fillStep1();
    await fillStep2();
    fillStep3();
    fillStep4();
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    await waitFor(() => expect(previewSpy).toHaveBeenCalledTimes(1));
    const [spec] = previewSpy.mock.calls[0];
    expect(spec.silver_write_mode).toBeUndefined();
    expect(spec.silver_bucket_count).toBeUndefined();
  });

  // --- Task 7: JDBC polling-limitation caveat ---

  it("shows the JDBC polling caveat for a scheduled-jdbc source", async () => {
    render(<AddSourceWizard />);

    await screen.findByRole("option", { name: /^scheduled$/i });
    fireEvent.change(screen.getByLabelText(/^kind$/i), { target: { value: "scheduled" } });
    await screen.findByRole("option", { name: /^pg$/i });
    fireEvent.change(screen.getByLabelText(/^type$/i), { target: { value: "pg" } });

    // Advance to step 3, where the incrementing_col/timestamp_col/poll_ms
    // fields (and the caveat rendered alongside them) live.
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /next/i }));

    expect(await screen.findByText(/deletes are never captured/i)).toBeInTheDocument();
  });

  it("does not show the JDBC polling caveat for a cdc source", async () => {
    render(<AddSourceWizard />);

    // Defaults (cdc/mssql) are already what we want -- advance to step 3.
    await screen.findByLabelText(/create stopped/i);
    fireEvent.click(screen.getByRole("button", { name: /next/i }));
    fireEvent.click(screen.getByRole("button", { name: /next/i }));

    expect(await screen.findByLabelText(/^database$/i)).toBeInTheDocument();
    expect(screen.queryByText(/deletes are never captured/i)).not.toBeInTheDocument();
  });
});
