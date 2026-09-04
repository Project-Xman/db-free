// SOT: datetime-parsing, temporal-kind, db-datetime-text
import { getLocalTimeZone, parseAbsolute, parseDate, parseDateTime, parseTime, type DateValue, type Time } from "@internationalized/date";

export type TemporalKind = "date" | "time" | "datetime";

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_RE = /^\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/;
const DATETIME_RE = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/;
const ZONED_RE = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}(?::?\d{2})?)$/i;

// WHAT:  Which temporal control a column gets: from its declared SQL type first,
//        then from the shape of a sample value (engines without typed metadata).
// WHY:   `timestamp` wants date+time segments, `date` a calendar only, `time`
//        a clock only. Every adapter hands date-like values over as DB text.
// WHERE: src/lib/fields.ts, src/components/global/Field.tsx
export function temporalKind(typeName: string, sample?: string): TemporalKind | null {
  const t = typeName.toLowerCase();
  if (/timestamp|datetime/.test(t)) return "datetime";
  if (/\bdate\b|^date/.test(t)) return "date";
  if (/\btime\b|^time/.test(t)) return "time";
  return sample === undefined ? null : kindOfText(sample);
}

export function kindOfText(text: string): TemporalKind | null {
  const s = text.trim();
  if (DATE_RE.test(s)) return "date";
  if (TIME_RE.test(s)) return "time";
  if (DATETIME_RE.test(s) || ZONED_RE.test(s)) return "datetime";
  return null;
}

// WHAT:  DB text → @internationalized/date value. `YYYY-MM-DD` → CalendarDate,
//        `YYYY-MM-DD HH:MM:SS[.f]` → CalendarDateTime, with an offset/Z →
//        ZonedDateTime in the local zone. Unparseable text → null.
export function parseDbDate(text: string): DateValue | null {
  const s = text.trim();
  const iso = s.replace(" ", "T");
  try {
    if (DATE_RE.test(s)) return parseDate(s);
    if (ZONED_RE.test(s)) return parseAbsolute(iso, getLocalTimeZone());
    if (DATETIME_RE.test(s)) return parseDateTime(iso);
  } catch {
    return null;
  }
  return null;
}

export function parseDbTime(text: string): Time | null {
  const s = text.trim();
  try {
    return TIME_RE.test(s) ? parseTime(s) : null;
  } catch {
    return null;
  }
}

const two = (n: number) => String(n).padStart(2, "0");

export function formatDbTime(t: { hour: number; minute: number; second: number }): string {
  return `${two(t.hour)}:${two(t.minute)}:${two(t.second)}`;
}

// WHAT:  Value → DB text. Zoned values go back as ISO-8601 UTC (what
//        timestamptz columns accept); naive ones keep the original separator.
export function formatDbDate(v: DateValue, separator: " " | "T" = " "): string {
  const date = `${String(v.year).padStart(4, "0")}-${two(v.month)}-${two(v.day)}`;
  if ("timeZone" in v) return v.toAbsoluteString();
  if ("hour" in v) return `${date}${separator}${formatDbTime(v)}`;
  return date;
}
