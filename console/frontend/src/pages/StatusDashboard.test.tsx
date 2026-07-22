import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import StatusDashboard from "./StatusDashboard";

describe("StatusDashboard", () => {
  it("renders connector states, dlq, and maintenance flags fetched from the API", async () => {
    vi.spyOn(client, "getStatus").mockResolvedValue({
      connectors: [
        {
          name: "dbz-mssql1-students",
          state: "RUNNING",
          maintenance: false,
          dlq: false,
          lag: null,
          reachable: true,
        },
        {
          name: "jdbc-pg-grades",
          state: "PAUSED",
          maintenance: true,
          dlq: true,
          lag: null,
          reachable: true,
        },
      ],
      reachable: true,
    });

    render(<StatusDashboard />);

    expect(await screen.findByText("dbz-mssql1-students")).toBeInTheDocument();
    expect(screen.getByText("jdbc-pg-grades")).toBeInTheDocument();
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
    expect(screen.getByText("PAUSED")).toBeInTheDocument();
    // maintenance/dlq flags rendered as Yes/No text
    expect(screen.getAllByText("Yes").length).toBeGreaterThan(0);
    expect(screen.getAllByText("No").length).toBeGreaterThan(0);
  });

  it("shows a degraded-connect banner when the backend reports unreachable", async () => {
    vi.spyOn(client, "getStatus").mockResolvedValue({
      connectors: [],
      reachable: false,
      error: "connect worker unreachable",
    });

    render(<StatusDashboard />);

    expect(await screen.findByText(/connect worker unreachable/i)).toBeInTheDocument();
  });

  it("shows an error message when the API call itself fails", async () => {
    vi.spyOn(client, "getStatus").mockRejectedValue(new Error("network down"));

    render(<StatusDashboard />);

    expect(await screen.findByText(/network down/i)).toBeInTheDocument();
  });
});
