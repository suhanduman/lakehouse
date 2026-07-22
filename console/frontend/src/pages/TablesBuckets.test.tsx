import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import TablesBuckets from "./TablesBuckets";

describe("TablesBuckets", () => {
  it("renders namespaces/tables and buckets fetched from the API", async () => {
    vi.spyOn(client, "listTables").mockResolvedValue({
      catalog: "lakehouse",
      namespaces: [
        { name: "students", tables: ["grades", "attendance"] },
        { name: "hr", tables: ["employees"] },
      ],
    });
    vi.spyOn(client, "listBuckets").mockResolvedValue(["lakehouse-students", "lakehouse-hr"]);

    render(<TablesBuckets />);

    expect(await screen.findByText("students")).toBeInTheDocument();
    expect(screen.getByText("grades")).toBeInTheDocument();
    expect(screen.getByText("attendance")).toBeInTheDocument();
    expect(screen.getByText("hr")).toBeInTheDocument();
    expect(screen.getByText("employees")).toBeInTheDocument();
    expect(screen.getByText("lakehouse-students")).toBeInTheDocument();
    expect(screen.getByText("lakehouse-hr")).toBeInTheDocument();
  });

  it("shows an error message when the tables API call fails", async () => {
    vi.spyOn(client, "listTables").mockRejectedValue(new Error("trino down"));
    vi.spyOn(client, "listBuckets").mockResolvedValue([]);

    render(<TablesBuckets />);

    expect(await screen.findByText(/trino down/i)).toBeInTheDocument();
  });

  it("shows an error message when the buckets API call fails", async () => {
    vi.spyOn(client, "listTables").mockResolvedValue({ catalog: "lakehouse", namespaces: [] });
    vi.spyOn(client, "listBuckets").mockRejectedValue(new Error("s3 down"));

    render(<TablesBuckets />);

    expect(await screen.findByText(/s3 down/i)).toBeInTheDocument();
  });
});
