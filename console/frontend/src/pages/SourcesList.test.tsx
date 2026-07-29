import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import SourcesList from "./SourcesList";

describe("SourcesList", () => {
  it("renders sources fetched from the API", async () => {
    vi.spyOn(client, "listSources").mockResolvedValue([
      {
        name: "dbz-mssql1-students",
        class: "io.debezium.connector.sqlserver.SqlServerConnector",
        paused: false,
        state: "RUNNING",
        cr_kind: "KafkaConnector",
      },
      {
        name: "jdbc-pg-grades",
        class: "io.confluent.connect.jdbc.JdbcSourceConnector",
        paused: true,
        state: "PAUSED",
        cr_kind: "KafkaConnector",
      },
    ]);

    render(<SourcesList />);

    expect(await screen.findByText("dbz-mssql1-students")).toBeInTheDocument();
    expect(screen.getByText("jdbc-pg-grades")).toBeInTheDocument();
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
    expect(screen.getByText("PAUSED")).toBeInTheDocument();
    expect(
      screen.getByText("io.debezium.connector.sqlserver.SqlServerConnector"),
    ).toBeInTheDocument();
  });

  it("distinguishes spark-batch sources from connectors via cr_kind", async () => {
    vi.spyOn(client, "listSources").mockResolvedValue([
      {
        name: "dbz-mssql1-students",
        class: "io.debezium.connector.sqlserver.SqlServerConnector",
        paused: false,
        state: "RUNNING",
        cr_kind: "KafkaConnector",
      },
      {
        name: "s3-batch-invoices",
        class: "ScheduledSparkApplication",
        paused: false,
        state: "Ready",
        cr_kind: "ScheduledSparkApplication",
        spark: {
          source: "s3-batch-invoices",
          target_ns: "rawlake",
          target_table: "invoices",
          s3_bucket: "raw-bucket",
          s3_prefix: "invoices/",
          file_format: "parquet",
          cron: "0 * * * *",
        },
      },
    ]);

    render(<SourcesList />);

    expect(await screen.findByText("dbz-mssql1-students")).toBeInTheDocument();
    expect(screen.getByText("s3-batch-invoices")).toBeInTheDocument();
    expect(screen.getByText("Spark Batch")).toBeInTheDocument();
  });

  it("shows an error message when the API call fails", async () => {
    vi.spyOn(client, "listSources").mockRejectedValue(new Error("network down"));

    render(<SourcesList />);

    expect(await screen.findByText(/network down/i)).toBeInTheDocument();
  });
});
