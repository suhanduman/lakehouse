import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import PipelineMap from "./PipelineMap";

function renderMap() {
  return render(
    <MemoryRouter>
      <PipelineMap />
    </MemoryRouter>,
  );
}

const ENTITY_PIPELINE: client.Pipeline = {
  name: "pgdemo",
  cr_kind: "KafkaConnector",
  disposition: "entity",
  authoritative: { fqn: "lakehouse.pgdemo.customers", layer: "silver" },
  nodes: [
    { type: "source", name: "pgdemo" },
    { type: "connector", name: "dbz-pgdemo-customers", kind: "io.debezium.connector.postgresql.PostgresConnector", state: "RUNNING" },
    { type: "topic", name: "pgdemo.public.customers" },
    { type: "sink", name: "sink-pgdemo-customers", state: "FAILED" },
    { type: "bronze", fqn: "rawlake.pgdemo_bronze.customers" },
    { type: "merge", name: "silver-merge", state: "PAUSED" },
    { type: "silver", fqn: "lakehouse.pgdemo.customers" },
    { type: "buckets", buckets: ["pgdemo-bronze", "pgdemo-silver"] },
  ],
  owned_tables: ["rawlake.pgdemo_bronze.customers", "lakehouse.pgdemo.customers"],
  owned_buckets: ["pgdemo-bronze", "pgdemo-silver"],
};

const ERROR_PIPELINE: client.Pipeline = {
  name: "broken-source",
  cr_kind: "KafkaConnector",
  error: "cannot resolve target namespace/table: neither CR carries transforms.route.static.value",
  nodes: [],
};

function mockClipboard() {
  const writeText = vi.fn().mockResolvedValue(undefined);
  Object.assign(navigator, { clipboard: { writeText } });
  return writeText;
}

describe("PipelineMap", () => {
  it("renders an entity pipeline's ordered node labels, a status dot, disposition badge, and the starred authoritative FQN with a working copy button", async () => {
    mockClipboard();
    vi.spyOn(client, "getPipelines").mockResolvedValue({ pipelines: [ENTITY_PIPELINE] });
    const { container } = renderMap();

    expect(await screen.findByText("pgdemo")).toBeInTheDocument();
    expect(screen.getByText(/entity/i)).toBeInTheDocument();

    // ordered node labels
    expect(screen.getByText(/dbz-pgdemo-customers/)).toBeInTheDocument();
    expect(screen.getByText(/pgdemo\.public\.customers/)).toBeInTheDocument();
    expect(screen.getByText(/sink-pgdemo-customers/)).toBeInTheDocument();
    expect(screen.getByText(/rawlake\.pgdemo_bronze\.customers/)).toBeInTheDocument();
    expect(screen.getByText(/silver-merge/)).toBeInTheDocument();

    // status dots: RUNNING (connector), FAILED (sink), PAUSED (merge)
    expect(screen.getByTitle("RUNNING")).toBeInTheDocument();
    expect(screen.getByTitle("FAILED")).toBeInTheDocument();
    expect(screen.getByTitle("PAUSED")).toBeInTheDocument();

    // authoritative FQN (in its own <code>) + copy button
    const authoritativeCode = container.querySelector("code");
    expect(authoritativeCode).toHaveTextContent("lakehouse.pgdemo.customers");
    expect(container.querySelector('[aria-hidden="true"]')).toHaveTextContent("⭐");

    const copyButton = screen.getByRole("button", { name: /copy/i });
    fireEvent.click(copyButton);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith("lakehouse.pgdemo.customers");
  });

  it("renders a pipeline's error inline instead of a full node chain", async () => {
    vi.spyOn(client, "getPipelines").mockResolvedValue({ pipelines: [ERROR_PIPELINE] });
    renderMap();

    expect(await screen.findByText("broken-source")).toBeInTheDocument();
    expect(screen.getByText(/cannot resolve target namespace\/table/)).toBeInTheDocument();
  });

  it("shows an empty state when there are no pipelines", async () => {
    vi.spyOn(client, "getPipelines").mockResolvedValue({ pipelines: [] });
    renderMap();
    expect(await screen.findByText(/no pipelines yet/i)).toBeInTheDocument();
  });

  it("fetch error renders an alert", async () => {
    vi.spyOn(client, "getPipelines").mockRejectedValue(new Error("boom"));
    renderMap();
    expect(await screen.findByRole("alert")).toHaveTextContent(/Failed to load pipelines/i);
  });

  it("Refresh re-fetches the pipeline list", async () => {
    const spy = vi
      .spyOn(client, "getPipelines")
      .mockResolvedValueOnce({ pipelines: [] })
      .mockResolvedValueOnce({ pipelines: [ENTITY_PIPELINE] });
    renderMap();

    expect(await screen.findByText(/no pipelines yet/i)).toBeInTheDocument();
    expect(spy).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: /refresh/i }));

    expect(await screen.findByText("pgdemo")).toBeInTheDocument();
    expect(spy).toHaveBeenCalledTimes(2);
  });

  it("shows a dead-letter warning badge linking to the sink's source when the DLQ has records", async () => {
    vi.spyOn(client, "getPipelines").mockResolvedValue({ pipelines: [ENTITY_PIPELINE] });
    const dlqSpy = vi
      .spyOn(client, "getConnectorDlq")
      .mockResolvedValue({ has_dlq: true, count: 3 });
    renderMap();

    expect(await screen.findByText(/3/)).toBeInTheDocument();
    const link = screen.getByRole("link", { name: /dropped|dead-letter|3/i });
    expect(link).toHaveAttribute("href", expect.stringContaining("/sources/"));
    expect(link).toHaveAttribute(
      "href",
      expect.stringContaining(encodeURIComponent("sink-pgdemo-customers")),
    );
    expect(dlqSpy).toHaveBeenCalledWith("sink-pgdemo-customers", 0);
  });

  it("shows no warning badge when the DLQ is empty", async () => {
    vi.spyOn(client, "getPipelines").mockResolvedValue({ pipelines: [ENTITY_PIPELINE] });
    vi.spyOn(client, "getConnectorDlq").mockResolvedValue({ has_dlq: true, count: 0 });
    renderMap();

    await screen.findByText(ENTITY_PIPELINE.name);
    expect(screen.queryByText(/⚠/)).toBeNull();
  });

  it("shows no warning badge when the DLQ fetch fails or is unreachable", async () => {
    vi.spyOn(client, "getPipelines").mockResolvedValue({ pipelines: [ENTITY_PIPELINE] });
    vi.spyOn(client, "getConnectorDlq").mockRejectedValue(new Error("unreachable"));
    renderMap();

    await screen.findByText(ENTITY_PIPELINE.name);
    expect(screen.queryByText(/⚠/)).toBeNull();
  });
});
