import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { MemoryRouter } from "react-router-dom";
import Nav from "./Nav";

describe("Nav", () => {
  it("renders internal navigation links", () => {
    render(
      <MemoryRouter>
        <Nav />
      </MemoryRouter>,
    );

    expect(screen.getByRole("link", { name: /sources/i })).toHaveAttribute("href", "/");
    expect(screen.getByRole("link", { name: /add source/i })).toHaveAttribute(
      "href",
      "/sources/add",
    );
    expect(screen.getByRole("link", { name: /^tables/i })).toHaveAttribute("href", "/tables");
    expect(screen.getByRole("link", { name: /^schemas/i })).toHaveAttribute("href", "/schemas");
    expect(screen.getByRole("link", { name: /^status/i })).toHaveAttribute("href", "/status");
  });

  it("renders the three deep-dive external links with safe target/rel and default hrefs", () => {
    render(
      <MemoryRouter>
        <Nav />
      </MemoryRouter>,
    );

    const kafkaUi = screen.getByRole("link", { name: /kafka ui/i });
    const apicurioUi = screen.getByRole("link", { name: /apicurio ui/i });
    const trinoUi = screen.getByRole("link", { name: /trino ui/i });

    for (const link of [kafkaUi, apicurioUi, trinoUi]) {
      expect(link).toHaveAttribute("target", "_blank");
      expect(link).toHaveAttribute("rel", "noopener");
    }

    expect(kafkaUi).toHaveAttribute("href", "http://localhost:8080");
    expect(apicurioUi).toHaveAttribute("href", "http://localhost:8081");
    expect(trinoUi).toHaveAttribute("href", "http://localhost:8082");
  });
});
