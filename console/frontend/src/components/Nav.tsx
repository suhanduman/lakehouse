import { NavLink } from "react-router-dom";

/**
 * Deep-dive external link targets, per docs/superpowers/sdd/task-14-brief.md:
 * links out to the underlying tools' own UIs (Kafka Connect via Kafka UI,
 * Apicurio Registry's UI, Trino's web UI) for cases the console itself
 * doesn't surface (raw topic browsing, schema diffing, query plans/history).
 *
 * Resolved from Vite env vars (`console/frontend/src/vite-env.d.ts` declares
 * the `VITE_*` shape) so each deployment can point at its own ingress
 * without a rebuild-time constant; sensible localhost defaults keep `npm run
 * dev` usable out of the box against the docker-compose stack.
 */
const KAFKA_UI_URL = import.meta.env.VITE_KAFKA_UI_URL ?? "http://localhost:8080";
const APICURIO_UI_URL = import.meta.env.VITE_APICURIO_UI_URL ?? "http://localhost:8081";
const TRINO_UI_URL = import.meta.env.VITE_TRINO_UI_URL ?? "http://localhost:8082";

interface DeepDiveLink {
  label: string;
  href: string;
}

const DEEP_DIVE_LINKS: DeepDiveLink[] = [
  { label: "Kafka UI", href: KAFKA_UI_URL },
  { label: "Apicurio UI", href: APICURIO_UI_URL },
  { label: "Trino UI", href: TRINO_UI_URL },
];

export default function Nav() {
  return (
    <nav>
      <NavLink to="/" end>
        Sources
      </NavLink>
      <NavLink to="/sources/add">Add source</NavLink>
      <NavLink to="/tables">Tables &amp; buckets</NavLink>
      <NavLink to="/schemas">Schemas</NavLink>
      <NavLink to="/status">Status</NavLink>
      <NavLink to="/gitops">GitOps</NavLink>
      <NavLink to="/pipelines">Pipeline Map</NavLink>

      <span aria-hidden="true"> | </span>

      {DEEP_DIVE_LINKS.map(({ label, href }) => (
        <a key={label} href={href} target="_blank" rel="noopener">
          {label}
        </a>
      ))}
    </nav>
  );
}
