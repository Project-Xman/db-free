// SOT: engine-icon, brand-icons, database-logos, svg-renderers
import type { FC } from "react";
import type { Engine } from "@/lib/bindings";

interface EngineIconProps {
  engine: Engine | (string & {});
  size?: number;
  className?: string;
}

export const EngineIcon: FC<EngineIconProps> = ({ engine, size = 28, className = "" }) => {
  const normalized = engine.toLowerCase();

  switch (normalized) {
    case "postgres":
    case "postgresql":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <path
            d="M24 4C12.954 4 4 12.954 4 24s8.954 20 20 20 20-8.954 20-20S35.046 4 24 4Z"
            fill="#336791"
          />
          <path
            d="M33.5 17.5c-1.2-3.1-4.8-5.5-9.5-5.5-5.2 0-9.8 3.1-10.8 7.5-.9 3.8.4 8.2 3.8 10.5 1.5 1 2.5 2.7 2.8 4.5.3 1.8 1.5 3 3.2 3.5.5-1.5.8-3.2 1-5 .8-7.2 4-11.8 9.5-15.5Z"
            fill="#FFFFFF"
            opacity={0.95}
          />
          <path
            d="M21 21c0-1.1.9-2 2-2s2 .9 2 2-.9 2-2 2-2-.9-2-2Z"
            fill="#336791"
          />
          <path
            d="M26 31c-1.5 3-3.2 5.5-5.5 7.5 1.2.3 2.5.5 3.5.5 6.6 0 12-5.4 12-12 0-2.8-1-5.5-2.8-7.5-1.5 4.5-3.8 8.5-7.2 11.5Z"
            fill="#2B5B84"
          />
        </svg>
      );

    case "mysql":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#00758F" />
          <path
            d="M13 32c2.5-7 7.5-11 13-11.5 5.5-.5 10 2 11.5 5.5-3-1.5-6.5-1.5-10-.5-3.5 1-6.5 3-8.5 6.5-2 3.5-4 4-6 0Z"
            fill="#F29111"
          />
          <path
            d="M26 14c-1.5 0-3 .5-4.5 1.5 2 1.5 3.5 3.5 4.5 6 1.5-2.5 3.5-4 6-4.5-1.8-2-3.8-3-6-3Z"
            fill="#FFFFFF"
          />
          <circle cx="21" cy="18" r="1.5" fill="#00758F" />
        </svg>
      );

    case "mariadb":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#003545" />
          <path
            d="M15 32c1-6 4-10 8-12 3-1.5 6-1 8 1 2 2 3 5 2 9-1 4-4 7-8 7-4 0-8-2-10-5Z"
            fill="#C08236"
          />
          <path
            d="M24 16c-2 0-3.5 1.5-4 3.5 1.5.5 3 1.5 4 3 1-1.5 2.5-2.5 4-3-.5-2-2-3.5-4-3.5Z"
            fill="#E8B568"
          />
          <circle cx="21" cy="18" r="1.2" fill="#003545" />
        </svg>
      );

    case "mssql":
    case "sqlserver":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#CC292B" />
          <ellipse cx="24" cy="15" rx="13" ry="4.5" fill="#FFFFFF" fillOpacity="0.95" />
          <path
            d="M11 15v7c0 2.5 5.8 4.5 13 4.5s13-2 13-4.5v-7"
            stroke="#FFFFFF"
            strokeWidth="2.5"
            strokeLinecap="round"
          />
          <path
            d="M11 23v7c0 2.5 5.8 4.5 13 4.5s13-2 13-4.5v-7"
            stroke="#FFFFFF"
            strokeWidth="2.5"
            strokeLinecap="round"
          />
        </svg>
      );

    case "clickhouse":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#1C1C1E" />
          <g transform="translate(6, 10)">
            <rect x="2" y="10" width="3" height="8" rx="1.5" fill="#FFCC00" />
            <rect x="6" y="7" width="3" height="14" rx="1.5" fill="#FFCC00" />
            <rect x="10" y="4" width="3" height="20" rx="1.5" fill="#FFCC00" />
            <rect x="14" y="2" width="3" height="24" rx="1.5" fill="#FFCC00" />
            <rect x="18" y="5" width="3" height="18" rx="1.5" fill="#FF4F00" />
            <rect x="22" y="9" width="3" height="10" rx="1.5" fill="#FF4F00" />
            <rect x="26" y="6" width="3" height="16" rx="1.5" fill="#FFCC00" />
            <rect x="30" y="12" width="3" height="4" rx="1.5" fill="#FFCC00" />
          </g>
        </svg>
      );

    case "redis":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#D82C20" />
          <path
            d="M15 13h10c4.4 0 7.5 2.8 7.5 6.8 0 3.2-1.9 5.5-4.8 6.3L33 35h-5.5l-4.5-8.2H19.5V35H15V13Zm4.5 9h5.2c1.9 0 3.2-1.1 3.2-2.7 0-1.6-1.3-2.7-3.2-2.7h-5.2v5.4Z"
            fill="#FFFFFF"
          />
        </svg>
      );

    case "mongodb":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#0A2218" />
          <path
            d="M24 8s-9 9-9 17.5c0 7.2 4.8 12.2 9 14.5 4.2-2.3 9-7.3 9-14.5C33 17 24 8 24 8Z"
            fill="#13AA52"
          />
          <path
            d="M24 8v32c.5 0 1-.1 1.5-.3 3.8-2.1 7.5-6.7 7.5-14.2C33 17 24 8 24 8Z"
            fill="#116149"
          />
          <path
            d="M24.2 38.5c-.3 1.5-.7 2.8-1.2 3.5-.2.3-.5.5-.8.5-.2 0-.3-.1-.4-.3-.3-.4-.5-1.2-.6-2.2l3-.15Z"
            fill="#F0F8F3"
          />
        </svg>
      );

    case "libsql":
    case "turso":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#132B2C" />
          <path
            d="M13 18c0-3.5 3-6 7-6 1.5 0 3 .5 4 1.5 1-1 2.5-1.5 4-1.5 4 0 7 2.5 7 6 0 5-6 9-11 15-5-6-11-10-11-15Z"
            fill="#4FF8D2"
          />
          <path
            d="M18 14c-2.5-3-5-3.5-7-2 1 3 2.5 5 4.5 6.5L18 14Zm12 0c2.5-3 5-3.5 7-2-1 3-2.5 5-4.5 6.5L30 14Z"
            fill="#2BD9B4"
          />
        </svg>
      );

    case "val_town":
    case "valtown":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#171717" />
          <text
            x="24"
            y="31"
            fontFamily="system-ui, -apple-system, sans-serif"
            fontSize="22"
            fontWeight="bold"
            letterSpacing="-1.5"
            fill="#FFFFFF"
            textAnchor="middle"
          >
            vt
          </text>
        </svg>
      );

    case "cloudflare_d1":
    case "cloudflare":
    case "d1":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#F38020" />
          <g fill="#FFFFFF">
            <ellipse cx="24" cy="16" rx="12" ry="4" />
            <path d="M12 16v6c0 2.2 5.4 4 12 4s12-1.8 12-4v-6c0 2.2-5.4 4-12 4s-12-1.8-12-4Z" />
            <path d="M12 24v6c0 2.2 5.4 4 12 4s12-1.8 12-4v-6c0 2.2-5.4 4-12 4s-12-1.8-12-4Z" />
          </g>
        </svg>
      );

    case "supabase":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#1C1C1C" />
          <path
            d="M25.5 10.5c.3-.8 1.4-1 1.9-.3l10.8 13.8c.6.8.1 2-.9 2H24.5l-1.2 11.5c-.2.8-1.3 1.1-1.8.4L10.7 24.1c-.6-.8-.1-2 .9-2h12.7l1.2-11.6Z"
            fill="#3ECF8E"
          />
        </svg>
      );

    case "planetscale":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#000000" />
          <circle cx="24" cy="24" r="14" fill="#111111" />
          <path
            d="M13 28C19 22 28 16 35 20c-6 6-15 12-22 8Z"
            fill="#FFFFFF"
          />
          <circle cx="24" cy="24" r="6" fill="#FFFFFF" />
        </svg>
      );

    case "neon":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#051914" />
          <path
            d="M15 34V14l18 20V14"
            stroke="#00E599"
            strokeWidth="4.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );

    case "sqlite":
    case "spatialite":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#003B57" />
          <path
            d="M32 14c-4 0-10 6-13 12l-3 8 7-3c6-3 12-9 12-13 0-2.5-1.5-4-3-4Z"
            fill="#29B6F6"
          />
          <path
            d="M16 34l-3 2 1-4 2 2Z"
            fill="#FFFFFF"
          />
        </svg>
      );

    case "qdrant":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#24386C" />
          <path d="M24 9l13 7.5v15L24 39l-13-7.5v-15L24 9Z" fill="#DC244C" />
          <path d="M24 9v30l13-7.5v-15L24 9Z" fill="#B2103A" />
          <path d="M24 17l7 4v8l-7 4-7-4v-8l7-4Z" fill="#FFFFFF" opacity={0.92} />
        </svg>
      );

    case "elasticsearch":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#1C1E23" />
          <path d="M10 18h22a10 10 0 0 1 0 6H10a12 12 0 0 1 0-6Z" fill="#FEC514" />
          <path d="M12 26h20a10 10 0 0 1-4 6H14a12 12 0 0 1-2-6Z" fill="#00BFB3" />
          <path d="M14 16h14a10 10 0 0 0-4-6H16a12 12 0 0 0-2 6Z" fill="#F04E98" />
        </svg>
      );

    case "cassandra":
    case "scylladb":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill={normalized === "scylladb" ? "#2E3440" : "#1287B1"} />
          <circle cx="24" cy="24" r="11" fill="none" stroke="#FFFFFF" strokeWidth="3" />
          <circle cx="24" cy="24" r="4" fill={normalized === "scylladb" ? "#57D1F5" : "#FFFFFF"} />
          <path d="M24 13v4M24 31v4M13 24h4M31 24h4" stroke="#FFFFFF" strokeWidth="2.5" strokeLinecap="round" />
        </svg>
      );

    case "neo4j":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#014063" />
          <circle cx="17" cy="30" r="5" fill="#018BFF" />
          <circle cx="31" cy="30" r="5" fill="#018BFF" />
          <circle cx="24" cy="16" r="5" fill="#018BFF" />
          <path d="M20 19l-1 6M28 19l1 6M22 30h4" stroke="#FFFFFF" strokeWidth="2" strokeLinecap="round" />
        </svg>
      );

    case "kafka":
    case "redpanda":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill={normalized === "redpanda" ? "#E3252B" : "#231F20"} />
          <circle cx="24" cy="12" r="4" fill="#FFFFFF" />
          <circle cx="24" cy="36" r="4" fill="#FFFFFF" />
          <circle cx="14" cy="24" r="4" fill="#FFFFFF" />
          <circle cx="34" cy="24" r="4" fill="#FFFFFF" />
          <path d="M24 16v16M18 24h12" stroke="#FFFFFF" strokeWidth="2.5" />
        </svg>
      );

    case "duckdb":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#FFF000" />
          <circle cx="22" cy="24" r="11" fill="#1A1A1A" />
          <path d="M31 21h9a2 2 0 0 1 0 4h-9v-4Z" fill="#1A1A1A" />
          <circle cx="18" cy="21" r="2" fill="#FFF000" />
        </svg>
      );

    case "snowflake":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#29B5E8" />
          <path d="M24 10v28M12 17l24 14M12 31l24-14" stroke="#FFFFFF" strokeWidth="3" strokeLinecap="round" />
          <path d="M20 12l4 4 4-4M20 36l4-4 4 4M10 20l5 2-1 5M38 20l-5 2 1 5M10 28l5-2-1-5M38 28l-5-2 1-5" stroke="#FFFFFF" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );

    default:
      return <Monogram engine={normalized} size={size} className={className} />;
  }
};

// WHAT:  Brand-coloured monogram tile for engines without a bespoke glyph.
// WHY:   Sixty-plus engines; every one still gets a distinct, recognisable tile.
const BRAND: Record<string, { bg: string; fg: string; text: string }> = {
  oracle: { bg: "#C74634", fg: "#FFFFFF", text: "O" },
  couchdb: { bg: "#E42528", fg: "#FFFFFF", text: "Cdb" },
  firestore: { bg: "#FFA000", fg: "#1F1F1F", text: "Fs" },
  valkey: { bg: "#6C3AD4", fg: "#FFFFFF", text: "Vk" },
  dynamodb: { bg: "#2E27AD", fg: "#FFFFFF", text: "Dy" },
  hbase: { bg: "#BA160C", fg: "#FFFFFF", text: "Hb" },
  memgraph: { bg: "#FB6E00", fg: "#FFFFFF", text: "Mg" },
  tigergraph: { bg: "#F58020", fg: "#1F1F1F", text: "Tg" },
  timescaledb: { bg: "#FDB515", fg: "#1F1F1F", text: "Ts" },
  influxdb: { bg: "#22ADF6", fg: "#FFFFFF", text: "Ifx" },
  victoriametrics: { bg: "#621773", fg: "#FFFFFF", text: "VM" },
  prometheus: { bg: "#E6522C", fg: "#FFFFFF", text: "Pr" },
  questdb: { bg: "#D14671", fg: "#FFFFFF", text: "Qd" },
  milvus: { bg: "#00A1EA", fg: "#FFFFFF", text: "Mv" },
  weaviate: { bg: "#61BD73", fg: "#1F1F1F", text: "Wv" },
  pinecone: { bg: "#1C17FF", fg: "#FFFFFF", text: "Pc" },
  chroma: { bg: "#FFDE59", fg: "#1F1F1F", text: "Ch" },
  pgvector: { bg: "#336791", fg: "#FFFFFF", text: "pgv" },
  opensearch: { bg: "#005EB8", fg: "#FFFFFF", text: "Os" },
  meilisearch: { bg: "#FF5CAA", fg: "#FFFFFF", text: "Me" },
  typesense: { bg: "#D52783", fg: "#FFFFFF", text: "Ty" },
  arangodb: { bg: "#DDE072", fg: "#1F1F1F", text: "Ar" },
  surrealdb: { bg: "#FF00A0", fg: "#FFFFFF", text: "Su" },
  orientdb: { bg: "#F26722", fg: "#FFFFFF", text: "Or" },
  postgis: { bg: "#2F8F4E", fg: "#FFFFFF", text: "GIS" },
  memcached: { bg: "#3C7A3A", fg: "#FFFFFF", text: "Mc" },
  dragonfly: { bg: "#FF6A00", fg: "#FFFFFF", text: "Df" },
  druid: { bg: "#29F1FB", fg: "#1F1F1F", text: "Dr" },
  bigquery: { bg: "#4285F4", fg: "#FFFFFF", text: "BQ" },
  cockroachdb: { bg: "#6933FF", fg: "#FFFFFF", text: "Cr" },
  tidb: { bg: "#E30C34", fg: "#FFFFFF", text: "Ti" },
  yugabytedb: { bg: "#FF6E42", fg: "#FFFFFF", text: "Yb" },
  rocksdb: { bg: "#FFA400", fg: "#1F1F1F", text: "Rk" },
  immudb: { bg: "#2E8BC0", fg: "#FFFFFF", text: "Im" },
  qldb: { bg: "#8C4FFF", fg: "#FFFFFF", text: "Ql" },
  objectdb: { bg: "#3F51B5", fg: "#FFFFFF", text: "Ob" },
  ibm_ims: { bg: "#0F62FE", fg: "#FFFFFF", text: "IMS" },
  raima_rdm: { bg: "#00629B", fg: "#FFFFFF", text: "RDM" },
  basex: { bg: "#3B6EA5", fg: "#FFFFFF", text: "Bx" },
  existdb: { bg: "#5A9E3A", fg: "#FFFFFF", text: "eX" },
  apache_jena: { bg: "#4B8B3B", fg: "#FFFFFF", text: "Je" },
  graphdb: { bg: "#04AA6D", fg: "#FFFFFF", text: "Gd" },
  stardog: { bg: "#6B2C91", fg: "#FFFFFF", text: "Sd" },
  blazegraph: { bg: "#D9481C", fg: "#FFFFFF", text: "Bz" },
  virtuoso: { bg: "#1B4F72", fg: "#FFFFFF", text: "Vi" },
};

function Monogram({ engine, size, className }: { engine: string; size: number; className: string }) {
  const brand = BRAND[engine] ?? { bg: "#3A3A3F", fg: "#FFFFFF", text: engine.slice(0, 2) };
  const fontSize = brand.text.length >= 3 ? 15 : 19;
  return (
    <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className} aria-hidden="true">
      <rect width="48" height="48" rx="10" fill={brand.bg} />
      <text x="24" y="31" fontFamily="system-ui, -apple-system, sans-serif" fontSize={fontSize} fontWeight="bold" letterSpacing="-0.5" fill={brand.fg} textAnchor="middle">
        {brand.text}
      </text>
    </svg>
  );
}
