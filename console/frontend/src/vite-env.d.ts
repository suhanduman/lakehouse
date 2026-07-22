/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Base URL of the Kafka UI deployment, for Nav's deep-dive link. */
  readonly VITE_KAFKA_UI_URL?: string;
  /** Base URL of the Apicurio Registry UI deployment, for Nav's deep-dive link. */
  readonly VITE_APICURIO_UI_URL?: string;
  /** Base URL of the Trino UI deployment, for Nav's deep-dive link. */
  readonly VITE_TRINO_UI_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
