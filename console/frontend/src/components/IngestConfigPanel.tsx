import { useState } from "react";
import type { IngestConfig } from "../api/client";

type Collector = keyof IngestConfig["snippets"];

const COLLECTOR_LABELS: Record<Collector, string> = {
  fluentbit: "Fluent Bit",
  vector: "Vector",
  logstash: "Logstash",
  generic: "Generic",
};

const COLLECTORS = Object.keys(COLLECTOR_LABELS) as Collector[];

/**
 * Shared Kafka log-ingestion config panel (Task 8, also consumed by Task 9's
 * source-detail view), rendered from `GET /api/sources/{name}/ingest-config`
 * (`app.routers.sources.get_ingest_config`, mirrored by the `IngestConfig`
 * type in `../api/client`).
 *
 * - A collector picker chooses which of `config.snippets[...]` to show, each
 *   a ready-to-paste config for that collector against this source's topic.
 * - `authoritative_fqn` is `Optional[str]` backend-side -- the pipeline may
 *   not have a resolved authoritative table yet (e.g. Silver merge not
 *   provisioned) -- so it's null-checked here rather than assumed present.
 * - The producer credential is never rendered before the user explicitly
 *   reveals it (`revealed` starts false); when the backend hasn't minted the
 *   secret yet (`producer.password === null`) reveal shows the `secret_ref`
 *   pointer instead of a password that doesn't exist.
 */
export default function IngestConfigPanel({ config }: { config: IngestConfig }) {
  const [collector, setCollector] = useState<Collector>("fluentbit");
  const [snippetCopied, setSnippetCopied] = useState(false);
  const [revealed, setRevealed] = useState(false);
  const [passwordCopied, setPasswordCopied] = useState(false);

  const snippet = config.snippets[collector];

  function handleCopySnippet() {
    navigator.clipboard.writeText(snippet);
    setSnippetCopied(true);
  }

  function handleCopyPassword() {
    if (config.producer.password === null) return;
    navigator.clipboard.writeText(config.producer.password);
    setPasswordCopied(true);
  }

  return (
    <div>
      <h3>Ingestion config</h3>
      <p>
        Topic: <code>{config.topic}</code> · Bootstrap:{" "}
        <code>{config.external_bootstrap}</code> · Disposition: {config.disposition}
      </p>

      <div>
        <label htmlFor="ingest-collector">Collector</label>
        <select
          id="ingest-collector"
          value={collector}
          onChange={(e) => {
            setCollector(e.target.value as Collector);
            setSnippetCopied(false);
          }}
        >
          {COLLECTORS.map((c) => (
            <option key={c} value={c}>
              {COLLECTOR_LABELS[c]}
            </option>
          ))}
        </select>
      </div>

      <pre>
        <code>{snippet}</code>
      </pre>
      <button type="button" onClick={handleCopySnippet}>
        Copy
      </button>
      {snippetCopied && <span> Copied</span>}

      <p>
        Authoritative table (records land here — query it):{" "}
        {config.authoritative_fqn !== null ? (
          <code>{config.authoritative_fqn}</code>
        ) : (
          <em>pipeline not resolved yet</em>
        )}
      </p>

      <div>
        <p>
          Producer credential ({config.producer.user} / {config.producer.mechanism})
        </p>
        {!revealed ? (
          <button type="button" onClick={() => setRevealed(true)}>
            Göster / Reveal
          </button>
        ) : config.producer.password !== null ? (
          <>
            <code>{config.producer.password}</code>
            <button type="button" onClick={handleCopyPassword}>
              Copy
            </button>
            {passwordCopied && <span> Copied</span>}
          </>
        ) : (
          <p>
            secret not ready — see <code>{config.producer.secret_ref}</code>
          </p>
        )}
      </div>

      {config.expected_json !== null && (
        <div>
          <p>Expected JSON shape:</p>
          <pre>{JSON.stringify(config.expected_json, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}
