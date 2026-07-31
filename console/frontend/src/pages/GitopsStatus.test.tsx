import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import GitopsStatus from "./GitopsStatus";

describe("GitopsStatus", () => {
  it("direct mode shows the not-enabled note and no table", async () => {
    vi.spyOn(client, "getGitopsStatus").mockResolvedValue({
      mode: "direct",
    } as client.GitopsStatusResponse);
    render(<GitopsStatus />);
    expect(await screen.findByText(/not enabled \(direct mode\)/i)).toBeInTheDocument();
    expect(screen.queryByRole("table")).toBeNull();
  });

  it("gitops mode renders app summary, a per-source row, and the drift list", async () => {
    vi.spyOn(client, "getGitopsStatus").mockResolvedValue({
      mode: "gitops",
      application: { sync: "Synced", health: "Healthy" },
      sources: [
        {
          source: "pgdemo",
          sync: "Synced",
          health: "Healthy",
          resources: [
            { kind: "KafkaConnector", name: "dbz-pgdemo-customers", status: "Synced", health: "Healthy" },
          ],
        },
      ],
      outOfSync: [{ kind: "KafkaConnector", name: "dbz-x-y", status: "OutOfSync", health: "Missing" }],
    });
    render(<GitopsStatus />);
    expect(await screen.findByText("pgdemo")).toBeInTheDocument();
    expect(screen.getByText(/dbz-pgdemo-customers/)).toBeInTheDocument();
    expect(screen.getByText(/dbz-x-y/)).toBeInTheDocument(); // in the drift list
  });

  it("gitops mode with null application shows the no-status-yet note", async () => {
    vi.spyOn(client, "getGitopsStatus").mockResolvedValue({
      mode: "gitops",
      application: null,
      sources: [],
      outOfSync: [],
    });
    render(<GitopsStatus />);
    expect(await screen.findByText(/No ArgoCD Application status yet/i)).toBeInTheDocument();
  });

  it("fetch error renders an alert", async () => {
    vi.spyOn(client, "getGitopsStatus").mockRejectedValue(new Error("boom"));
    render(<GitopsStatus />);
    expect(await screen.findByRole("alert")).toHaveTextContent(/Failed to load GitOps status/i);
  });
});
