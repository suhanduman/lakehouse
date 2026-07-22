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
      },
      {
        name: "jdbc-pg-grades",
        class: "io.confluent.connect.jdbc.JdbcSourceConnector",
        paused: true,
        state: "PAUSED",
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

  it("shows an error message when the API call fails", async () => {
    vi.spyOn(client, "listSources").mockRejectedValue(new Error("network down"));

    render(<SourcesList />);

    expect(await screen.findByText(/network down/i)).toBeInTheDocument();
  });
});
