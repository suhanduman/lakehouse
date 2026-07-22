import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import AddSourceWizard from "./AddSourceWizard";

const PREVIEW_RESPONSE = {
  bucket: "src-mssql-ogrenci",
  namespace_ddl:
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.mssql_ogrenci WITH (location='s3://src-mssql-ogrenci/warehouse')",
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

function fillStep2() {
  fireEvent.change(screen.getByLabelText(/source name/i), {
    target: { value: "mssql1" },
  });
  fireEvent.change(screen.getByLabelText(/database host/i), {
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
  fireEvent.change(screen.getByLabelText(/target namespace/i), {
    target: { value: "mssql_ogrenci" },
  });
  fireEvent.change(screen.getByLabelText(/target table/i), {
    target: { value: "students" },
  });
  fireEvent.click(screen.getByRole("button", { name: /next/i }));
}

async function fillThroughPreview() {
  fillStep1();
  fillStep2();
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

    // Preview shows connector class, topic name, bucket, and DDL.
    expect(
      screen.getByText("io.debezium.connector.sqlserver.SqlServerConnector"),
    ).toBeInTheDocument();
    expect(screen.getByText("cdc.mssql1.dbo.students")).toBeInTheDocument();
    expect(screen.getByText("src-mssql-ogrenci")).toBeInTheDocument();
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
      },
      { user: "sa", password: "s3cret" },
    );

    expect(await screen.findByText(/source created/i)).toBeInTheDocument();
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
    fillStep2();
    fillStep3();
    fillStep4();
    fireEvent.click(screen.getByRole("button", { name: /fetch preview/i }));

    expect(await screen.findByText(/boom/i)).toBeInTheDocument();
    expect(createSpy).not.toHaveBeenCalled();
  });
});
