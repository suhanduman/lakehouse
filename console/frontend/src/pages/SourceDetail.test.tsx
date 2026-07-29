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
});
