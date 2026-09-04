// SOT: engine-icon, brand-icons, database-logos, svg-renderers
import type { FC } from "react";
import {
  siApache,
  siApachecassandra,
  siApachecouchdb,
  siApachedruid,
  siApachehbase,
  siApachekafka,
  siArangodb,
  siClickhouse,
  siCloudflare,
  siCockroachlabs,
  siDuckdb,
  siElasticsearch,
  siFirebase,
  siGooglebigquery,
  siInfluxdb,
  siMariadb,
  siMeilisearch,
  siMilvus,
  siMongodb,
  siMysql,
  siNeo4j,
  siNeon,
  siOpensearch,
  siPlanetscale,
  siPostgresql,
  siPrometheus,
  siQdrant,
  siRedis,
  siRocksdb,
  siScylladb,
  siSnowflake,
  siSqlite,
  siSupabase,
  siSurrealdb,
  siTidb,
  siTimescale,
  siTurso,
  siVictoriametrics,
  type SimpleIcon,
} from "simple-icons";
import type { Engine } from "@/lib/bindings";

interface EngineIconProps {
  engine: Engine;
  size?: number;
  className?: string;
}

// WHAT:  Real brand marks (simple-icons, CC0, bundled — no CDN) keyed by engine.
//        Products that only speak another engine's wire protocol borrow its mark
//        (pgvector / PostGIS → PostgreSQL, SpatiaLite → SQLite, Jena → Apache).
// WHY:   "Which database is this?" should be answerable from the tile alone.
//        Engines without a published mark fall back to a hand-drawn tile or a
//        brand-coloured monogram below.
const BRAND_MARKS: Partial<Record<Engine, SimpleIcon>> = {
  postgres: siPostgresql,
  pgvector: siPostgresql,
  postgis: siPostgresql,
  mysql: siMysql,
  mariadb: siMariadb,
  mongodb: siMongodb,
  couchdb: siApachecouchdb,
  firestore: siFirebase,
  redis: siRedis,
  cassandra: siApachecassandra,
  scylladb: siScylladb,
  hbase: siApachehbase,
  neo4j: siNeo4j,
  timescaledb: siTimescale,
  influxdb: siInfluxdb,
  victoriametrics: siVictoriametrics,
  prometheus: siPrometheus,
  qdrant: siQdrant,
  milvus: siMilvus,
  elasticsearch: siElasticsearch,
  opensearch: siOpensearch,
  meilisearch: siMeilisearch,
  arangodb: siArangodb,
  surrealdb: siSurrealdb,
  sqlite: siSqlite,
  spatialite: siSqlite,
  clickhouse: siClickhouse,
  duckdb: siDuckdb,
  druid: siApachedruid,
  snowflake: siSnowflake,
  bigquery: siGooglebigquery,
  cockroachdb: siCockroachlabs,
  tidb: siTidb,
  rocksdb: siRocksdb,
  libsql: siTurso,
  cloudflare_d1: siCloudflare,
  supabase: siSupabase,
  planetscale: siPlanetscale,
  neon: siNeon,
  kafka: siApachekafka,
  apache_jena: siApache,
};

// WHAT:  Brand tile: the official glyph on its brand colour; light brands get a
//        dark glyph so the mark stays legible.
function BrandTile({ icon, size, className }: { icon: SimpleIcon; size: number; className: string }) {
  const glyph = luminance(icon.hex) > 0.55 ? "#1F1F1F" : "#FFFFFF";
  return (
    <svg width={size} height={size} viewBox="0 0 48 48" className={className} aria-hidden="true">
      <rect width="48" height="48" rx="10" fill={`#${icon.hex}`} />
      <path d={icon.path} fill={glyph} transform="translate(9 9) scale(1.25)" />
    </svg>
  );
}

// Relative luminance (WCAG) of a 6-digit hex colour, 0 = black, 1 = white.
function luminance(hex: string): number {
  const channel = (i: number) => {
    const c = Number.parseInt(hex.slice(i, i + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * channel(0) + 0.7152 * channel(2) + 0.0722 * channel(4);
}

export const EngineIcon: FC<EngineIconProps> = ({ engine, size = 28, className = "" }) => {
  const mark = BRAND_MARKS[engine];
  if (mark) return <BrandTile icon={mark} size={size} className={className} />;
  const normalized: string = engine;

  switch (normalized) {

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

    case "redpanda":
      return (
        <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className={className}>
          <rect width="48" height="48" rx="10" fill="#E3252B" />
          <circle cx="24" cy="12" r="4" fill="#FFFFFF" />
          <circle cx="24" cy="36" r="4" fill="#FFFFFF" />
          <circle cx="14" cy="24" r="4" fill="#FFFFFF" />
          <circle cx="34" cy="24" r="4" fill="#FFFFFF" />
          <path d="M24 16v16M18 24h12" stroke="#FFFFFF" strokeWidth="2.5" />
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
