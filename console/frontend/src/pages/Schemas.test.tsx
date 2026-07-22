import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import Schemas from "./Schemas";

describe("Schemas", () => {
  it("renders schemas fetched from the Apicurio registry API", async () => {
    vi.spyOn(client, "listSchemas").mockResolvedValue([
      { name: "students-grades-value", id: "1", type: "AVRO" },
      { name: "hr-employees-value", id: "2", type: "JSON" },
    ]);

    render(<Schemas />);

    expect(await screen.findByText("students-grades-value")).toBeInTheDocument();
    expect(screen.getByText("hr-employees-value")).toBeInTheDocument();
    expect(screen.getByText("AVRO")).toBeInTheDocument();
    expect(screen.getByText("JSON")).toBeInTheDocument();
  });

  it("shows a message when there are no registered schemas", async () => {
    vi.spyOn(client, "listSchemas").mockResolvedValue([]);

    render(<Schemas />);

    expect(await screen.findByText(/no schemas registered/i)).toBeInTheDocument();
  });

  it("shows an error message when the API call fails", async () => {
    vi.spyOn(client, "listSchemas").mockRejectedValue(new Error("apicurio unreachable"));

    render(<Schemas />);

    expect(await screen.findByText(/apicurio unreachable/i)).toBeInTheDocument();
  });
});
