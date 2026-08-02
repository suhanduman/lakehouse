import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as client from "../api/client";
import TablesBuckets from "./TablesBuckets";

const USED_TABLE: client.TableEntry = {
  table: "grades",
  used: true,
  pipeline: "students-ingest",
  role: "silver",
  records: 42,
};

const ORPHAN_TABLE: client.TableEntry = {
  table: "attendance",
  used: false,
  hint: "no active pipeline — leftover from a deleted pipeline or hand-created",
  records: null,
};

const USED_BUCKET: client.BucketEntry = {
  name: "students-bronze",
  used: true,
  pipeline: "students-ingest",
  role: "bronze",
  objects: { count: 7, capped: false },
};

const ORPHAN_BUCKET: client.BucketEntry = {
  name: "leftover-bucket",
  used: false,
  hint: "no active pipeline — leftover from a deleted pipeline or hand-created",
  objects: { count: 1000, capped: true },
};

describe("TablesBuckets", () => {
  it("renders namespaces/tables and buckets fetched from the API, with used/orphan badges and counts", async () => {
    vi.spyOn(client, "listTables").mockResolvedValue({
      catalog: "lakehouse",
      namespaces: [{ name: "students", tables: [USED_TABLE, ORPHAN_TABLE] }],
    });
    vi.spyOn(client, "listBuckets").mockResolvedValue([USED_BUCKET, ORPHAN_BUCKET]);

    render(<TablesBuckets />);

    expect(await screen.findByText("students")).toBeInTheDocument();
    expect(screen.getByText("grades")).toBeInTheDocument();
    expect(screen.getByText("attendance")).toBeInTheDocument();
    expect(screen.getByText("students-bronze")).toBeInTheDocument();
    expect(screen.getByText("leftover-bucket")).toBeInTheDocument();

    // used table: record count + pipeline + role
    expect(screen.getByText(/42/)).toBeInTheDocument();
    const gradesRow = screen.getByText("grades").closest("li");
    expect(gradesRow).toHaveTextContent("students-ingest");
    expect(gradesRow).toHaveTextContent("silver");

    // orphan table: hint + count fallback ("—" for null records)
    const attendanceRow = screen.getByText("attendance").closest("li");
    expect(attendanceRow).toHaveTextContent(/no active pipeline/i);
    expect(attendanceRow).toHaveTextContent("—");

    // used bucket: object count + pipeline + role badge
    const usedBucketRow = screen.getByText("students-bronze").closest("li");
    expect(usedBucketRow).toHaveTextContent("7");
    expect(usedBucketRow).toHaveTextContent("students-ingest");
    expect(usedBucketRow).toHaveTextContent("bronze");

    // orphan bucket: object count with "+" (capped) + hint
    const orphanBucketRow = screen.getByText("leftover-bucket").closest("li");
    expect(orphanBucketRow).toHaveTextContent("1000+");
    expect(orphanBucketRow).toHaveTextContent(/no active pipeline/i);
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

  it("Refresh re-fetches both tables and buckets", async () => {
    const tablesSpy = vi
      .spyOn(client, "listTables")
      .mockResolvedValueOnce({ catalog: "lakehouse", namespaces: [] })
      .mockResolvedValueOnce({
        catalog: "lakehouse",
        namespaces: [{ name: "students", tables: [USED_TABLE] }],
      });
    const bucketsSpy = vi
      .spyOn(client, "listBuckets")
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([USED_BUCKET]);

    render(<TablesBuckets />);

    expect(await screen.findByText(/no namespaces found/i)).toBeInTheDocument();
    expect(tablesSpy).toHaveBeenCalledTimes(1);
    expect(bucketsSpy).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: /refresh/i }));

    expect(await screen.findByText("grades")).toBeInTheDocument();
    expect(screen.getByText("students-bronze")).toBeInTheDocument();
    expect(tablesSpy).toHaveBeenCalledTimes(2);
    expect(bucketsSpy).toHaveBeenCalledTimes(2);
  });
});
