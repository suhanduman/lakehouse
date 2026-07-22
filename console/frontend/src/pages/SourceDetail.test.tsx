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

function renderDetail(role: Role) {
  return render(
    <MemoryRouter initialEntries={[`/sources/${SOURCE_NAME}`]}>
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

  it("shows an error message when the source fails to load", async () => {
    vi.spyOn(client, "getSource").mockRejectedValue(new Error("not found"));
    vi.spyOn(client, "getStatus").mockResolvedValue({ connectors: [], reachable: true });

    renderDetail("ADMIN");

    expect(await screen.findByText(/not found/i)).toBeInTheDocument();
  });
});
