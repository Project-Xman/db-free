// SOT: icon-set, hugeicons, type-icons
// WHAT:  Hugeicons (stroke-rounded, free set) rendered inline as SVG. No icon
//        font, no CDN — the app must work air-gapped; only the imported glyphs
//        ship in the bundle.
import type { SVGProps } from "react";
import { HugeiconsIcon, type IconSvgElement } from "@hugeicons/react";
import {
  ArrowDown01Icon,
  ArrowDown02Icon,
  ArrowLeft01Icon,
  ArrowLeft02Icon,
  ArrowRight01Icon,
  ArrowShrink01Icon,
  ArrowUp02Icon,
  ArrowExpand01Icon,
  BinaryCodeIcon,
  Calendar03Icon,
  Cancel01Icon,
  Clock01Icon,
  ComputerTerminal01Icon,
  Copy01Icon,
  DatabaseIcon,
  Delete02Icon,
  Download04Icon,
  File01Icon,
  FilterIcon,
  Folder01Icon,
  HashtagIcon,
  HistoryIcon,
  Home01Icon,
  InformationCircleIcon,
  Key01Icon,
  LayoutThreeColumnIcon,
  Link04Icon,
  MinusSignIcon,
  PencilEdit02Icon,
  PlayIcon,
  PlugSocketIcon,
  PlusSignIcon,
  RefreshIcon,
  RowsThreeIcon,
  Search01Icon,
  Settings02Icon,
  Sorting05Icon,
  SourceCodeIcon,
  SquareLock02Icon,
  Table02Icon,
  TableIcon,
  TextFontIcon,
  Tick02Icon,
  ToggleOnIcon,
  ViewIcon,
  ViewOffSlashIcon,
} from "@hugeicons/core-free-icons";

export type IconName =
  | "database"
  | "table"
  | "view"
  | "terminal"
  | "plus"
  | "minus"
  | "refresh"
  | "search"
  | "filter"
  | "sort"
  | "arrow-up"
  | "arrow-down"
  | "download"
  | "chevron-down"
  | "chevron-right"
  | "chevron-left"
  | "x"
  | "key"
  | "text"
  | "hash"
  | "toggle"
  | "braces"
  | "calendar"
  | "binary"
  | "eye"
  | "eye-off"
  | "check"
  | "lock"
  | "arrow-left"
  | "columns"
  | "rows"
  | "settings"
  | "home"
  | "play"
  | "history"
  | "trash"
  | "pencil"
  | "plug"
  | "file"
  | "folder"
  | "link"
  | "info"
  | "clock"
  | "copy"
  | "expand"
  | "collapse";

// WHAT:  App icon vocabulary → Hugeicons (stroke-rounded, free set). Call sites
//        stay `<Icon name="table" />`; swap a glyph here, not in features.
const GLYPHS: Record<IconName, IconSvgElement> = {
  database: DatabaseIcon,
  table: TableIcon,
  view: Table02Icon,
  terminal: ComputerTerminal01Icon,
  plus: PlusSignIcon,
  minus: MinusSignIcon,
  refresh: RefreshIcon,
  search: Search01Icon,
  filter: FilterIcon,
  sort: Sorting05Icon,
  "arrow-up": ArrowUp02Icon,
  "arrow-down": ArrowDown02Icon,
  download: Download04Icon,
  "chevron-down": ArrowDown01Icon,
  "chevron-right": ArrowRight01Icon,
  "chevron-left": ArrowLeft01Icon,
  x: Cancel01Icon,
  key: Key01Icon,
  text: TextFontIcon,
  hash: HashtagIcon,
  toggle: ToggleOnIcon,
  braces: SourceCodeIcon,
  calendar: Calendar03Icon,
  binary: BinaryCodeIcon,
  eye: ViewIcon,
  "eye-off": ViewOffSlashIcon,
  check: Tick02Icon,
  lock: SquareLock02Icon,
  "arrow-left": ArrowLeft02Icon,
  columns: LayoutThreeColumnIcon,
  rows: RowsThreeIcon,
  settings: Settings02Icon,
  home: Home01Icon,
  play: PlayIcon,
  history: HistoryIcon,
  trash: Delete02Icon,
  pencil: PencilEdit02Icon,
  plug: PlugSocketIcon,
  file: File01Icon,
  folder: Folder01Icon,
  link: Link04Icon,
  info: InformationCircleIcon,
  clock: Clock01Icon,
  copy: Copy01Icon,
  expand: ArrowExpand01Icon,
  collapse: ArrowShrink01Icon,
};

interface IconProps extends Omit<SVGProps<SVGSVGElement>, "name" | "strokeWidth"> {
  name: IconName;
  size?: number;
}

export function Icon({ name, size = 15, className, ...rest }: IconProps) {
  return (
    <HugeiconsIcon
      icon={GLYPHS[name]}
      size={size}
      color="currentColor"
      strokeWidth={1.75}
      aria-hidden="true"
      className={["inline-block shrink-0 align-middle", className].filter((c) => c !== undefined).join(" ")}
      {...rest}
    />
  );
}

// WHAT:  Column-type glyph for grid headers and the tables tree.
export function typeIcon(dataType: string, primaryKey = false): IconName {
  if (primaryKey) return "key";
  const t = dataType.toLowerCase();
  if (t.includes("bool")) return "toggle";
  if (/int|serial|numeric|decimal|real|float|double|number|money/.test(t)) return "hash";
  if (t.includes("json")) return "braces";
  if (/date|time/.test(t)) return "calendar";
  if (/bytea|blob|binary/.test(t)) return "binary";
  return "text";
}
