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
  Activity01Icon,
  AiBrain01Icon,
  Alert01Icon,
  Archive01Icon,
  ArrowDataTransferHorizontalIcon,
  Book02Icon,
  Camera01Icon,
  ChartBarLineIcon,
  ChartLineData01Icon,
  ChartRelationshipIcon,
  CodeIcon,
  CodeXmlIcon,
  Compass01Icon,
  CpuIcon,
  CubeIcon,
  DatabaseSync01Icon,
  Exchange01Icon,
  FlashIcon,
  FlowConnectionIcon,
  FunctionIcon,
  GaugeIcon,
  GitBranchIcon,
  Globe02Icon,
  GridIcon,
  HierarchyIcon,
  Layers01Icon,
  ListViewIcon,
  LockKeyIcon,
  MapsIcon,
  Message01Icon,
  Notification01Icon,
  PackageIcon,
  Radar01Icon,
  RepeatIcon,
  Route01Icon,
  RssIcon,
  SearchListIcon,
  SendIcon,
  ServerIcon,
  Shield01Icon,
  SparklesIcon,
  SwatchIcon,
  Tag01Icon,
  Target01Icon,
  Task01Icon,
  Timer01Icon,
  UserGroupIcon,
  UserIcon,
  Wrench01Icon,
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
  | "collapse"
  | "function"
  | "user"
  | "users"
  | "shield"
  | "server"
  | "activity"
  | "layers"
  | "cpu"
  | "globe"
  | "flash"
  | "alert"
  | "package"
  | "code"
  | "chart"
  | "chart-bar"
  | "chart-relationship"
  | "gauge"
  | "hierarchy"
  | "tag"
  | "archive"
  | "flow"
  | "radar"
  | "sparkles"
  | "cube"
  | "route"
  | "send"
  | "bell"
  | "repeat"
  | "list"
  | "target"
  | "book"
  | "swatch"
  | "task"
  | "camera"
  | "database-sync"
  | "lock-key"
  | "wrench"
  | "grid"
  | "search-list"
  | "xml"
  | "message"
  | "timer"
  | "map"
  | "compass"
  | "git-branch"
  | "brain"
  | "exchange"
  | "transfer"
  | "rss";

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
  function: FunctionIcon,
  user: UserIcon,
  users: UserGroupIcon,
  shield: Shield01Icon,
  server: ServerIcon,
  activity: Activity01Icon,
  layers: Layers01Icon,
  cpu: CpuIcon,
  globe: Globe02Icon,
  flash: FlashIcon,
  alert: Alert01Icon,
  package: PackageIcon,
  code: CodeIcon,
  chart: ChartLineData01Icon,
  "chart-bar": ChartBarLineIcon,
  "chart-relationship": ChartRelationshipIcon,
  gauge: GaugeIcon,
  hierarchy: HierarchyIcon,
  tag: Tag01Icon,
  archive: Archive01Icon,
  flow: FlowConnectionIcon,
  radar: Radar01Icon,
  sparkles: SparklesIcon,
  cube: CubeIcon,
  route: Route01Icon,
  send: SendIcon,
  bell: Notification01Icon,
  repeat: RepeatIcon,
  list: ListViewIcon,
  target: Target01Icon,
  book: Book02Icon,
  swatch: SwatchIcon,
  task: Task01Icon,
  camera: Camera01Icon,
  "database-sync": DatabaseSync01Icon,
  "lock-key": LockKeyIcon,
  wrench: Wrench01Icon,
  grid: GridIcon,
  "search-list": SearchListIcon,
  xml: CodeXmlIcon,
  message: Message01Icon,
  timer: Timer01Icon,
  map: MapsIcon,
  compass: Compass01Icon,
  "git-branch": GitBranchIcon,
  brain: AiBrain01Icon,
  exchange: Exchange01Icon,
  transfer: ArrowDataTransferHorizontalIcon,
  rss: RssIcon,
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
