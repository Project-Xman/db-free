// SOT: form-fields, text-field, select-field, toggle-field, segmented-control, checkbox-field, datetime-field, number-input
import type { ReactNode } from "react";
import {
  Calendar,
  Checkbox as HeroCheckbox,
  DateField,
  DatePicker,
  Description,
  Input,
  InputGroup,
  Label,
  ListBox,
  NumberField,
  Select,
  Switch,
  Tabs,
  TextField,
  TimeField,
} from "@heroui/react";
import { cn } from "@/lib/cn";
import { Icon, type IconName } from "@/lib/icons";
import { formatDbDate, formatDbTime, parseDbDate, parseDbTime, type TemporalKind } from "@/lib/datetime";

// WHAT:  Thin typed wrappers over HeroUI form controls.
// WHY:   HeroUI selection APIs speak `Key`; features want their own string unions.
//        Wrapping once keeps every feature strictly typed without casts.
// WHERE: https://heroui.com/docs/react/components/{text-field,select,switch,checkbox,tabs}
// WHAT:  Controls fill their container unless the caller sets an explicit width.
function widthClass(className: string | undefined): string {
  return className !== undefined && /\bw-/.test(className) ? "" : "w-full";
}

interface FieldProps {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string | undefined;
  type?: "text" | "password" | "number" | "email";
  description?: string | undefined;
  optional?: boolean;
  isDisabled?: boolean;
  autoFocus?: boolean;
  className?: string;
  suffix?: ReactNode;
  mono?: boolean;
  /// Row-height control for dense surfaces (filter builder, toolbars): 28px
  /// input, 12px text, no label gap.
  compact?: boolean;
}

export function Field({ label, value, onChange, placeholder, type = "text", description, optional = false, isDisabled = false, autoFocus = false, className, suffix, mono = false, compact = false }: FieldProps) {
  const inputClass = cn("w-full", mono ? "font-mono" : "", compact ? "h-7 min-h-7 px-2 text-xs" : "");
  const placeholderProps = placeholder !== undefined ? { placeholder } : {};
  return (
    <TextField value={value} onChange={onChange} type={type} isDisabled={isDisabled} className={cn("w-full", compact ? "gap-0" : "", className)} autoFocus={autoFocus}>
      <Label>
        {label}
        {optional ? <span className="ml-1 text-muted">(optional)</span> : null}
      </Label>
      {suffix ? (
        <InputGroup>
          <InputGroup.Input {...placeholderProps} className={inputClass} />
          <InputGroup.Suffix>{suffix}</InputGroup.Suffix>
        </InputGroup>
      ) : (
        <Input {...placeholderProps} className={inputClass} />
      )}
      {description ? <Description>{description}</Description> : null}
    </TextField>
  );
}

export interface Option<T extends string> {
  value: T;
  label: string;
  icon?: IconName;
  /// Arbitrary leading node (an engine logo); wins over `icon`.
  leading?: ReactNode;
}

interface AppSelectProps<T extends string> {
  value: T;
  options: readonly Option<T>[];
  onChange: (value: T) => void;
  label?: string | undefined;
  ariaLabel?: string | undefined;
  className?: string | undefined;
  isDisabled?: boolean;
  size?: "sm" | "md";
  icon?: IconName | undefined;
  /// Borderless breadcrumb-style trigger (sidebar database / schema switcher).
  plain?: boolean;
}

export function AppSelect<T extends string>({ value, options, onChange, label, ariaLabel, className, isDisabled = false, size = "md", icon, plain = false }: AppSelectProps<T>) {
  const current = options.find((o) => o.value === value);
  const leading = icon ?? current?.icon;
  return (
    <Select
      value={value}
      onChange={(key) => {
        const next = options.find((o) => o.value === String(key));
        if (next) onChange(next.value);
      }}
      aria-label={label ?? ariaLabel ?? "Select"}
      isDisabled={isDisabled}
      className={cn(plain ? "w-auto" : widthClass(className), "min-w-0", className)}
    >
      {label ? <Label>{label}</Label> : null}
      <Select.Trigger
        className={cn(
          // Inline chevron + flex centring for every size: HeroUI's own indicator is absolutely
          // positioned, which drifts off the text baseline in compact triggers.
          "!inline-flex w-full min-w-0 !items-center gap-1.5 !pr-2.5 leading-normal",
          size === "sm" ? "h-7 min-h-7 px-2 text-xs" : "",
          plain ? "!w-auto h-6 min-h-6 gap-1 rounded-md border-0 bg-transparent !pr-1.5 !pl-1 py-0 text-xs text-foreground shadow-none hover:bg-surface-secondary" : "",
        )}
      >
        {icon === undefined && current?.leading ? <span className="flex shrink-0 items-center">{current.leading}</span> : leading ? <Icon name={leading} size={plain ? 12 : 14} className="shrink-0 text-accent" /> : null}
        {/* Select.Value renders the whole selected ListBox.Item by default, so an
            option with a leading logo would draw it a second time next to the one
            the trigger already shows. Render just the label in that case. */}
        <Select.Value className={cn("min-w-0 truncate text-left leading-normal", plain ? "max-w-[150px]" : "flex-1")}>
          {({ defaultChildren, isPlaceholder, state }) =>
            isPlaceholder || state.selectedItems.length === 0 ? defaultChildren : (current?.label ?? defaultChildren)
          }
        </Select.Value>
        <Icon name="chevron-down" size={12} className="shrink-0 text-muted" />
      </Select.Trigger>
      <Select.Popover>
        <ListBox>
          {options.map((o) => (
            <ListBox.Item key={o.value} id={o.value} textValue={o.label}>
              {o.leading ? <span className="mr-2 flex shrink-0 items-center">{o.leading}</span> : o.icon ? <Icon name={o.icon} size={14} className="mr-2 text-muted" /> : null}
              {o.label}
              <ListBox.ItemIndicator />
            </ListBox.Item>
          ))}
        </ListBox>
      </Select.Popover>
    </Select>
  );
}

interface ToggleProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
  description?: string;
}

export function Toggle({ checked, onChange, label, description }: ToggleProps) {
  // WHAT:  HeroUI's own anatomy: the control and the label live inside
  //        Switch.Content, with Description as a sibling.
  // WHY:   Rendering the label in a separate Button next to the Switch made the
  //        thumb overlap the text, and clicking the label did nothing for
  //        screen readers. Switch.Content is already the click target.
  // WHERE: https://heroui.com/docs/react/components/switch (with-description)
  return (
    <Switch isSelected={checked} onChange={onChange} aria-label={label.length > 0 ? label : "toggle"} className="w-full">
      <Switch.Content className="flex items-center gap-3">
        <Switch.Control>
          <Switch.Thumb />
        </Switch.Control>
        {label.length > 0 ? <span className="text-[13px] text-foreground">{label}</span> : null}
      </Switch.Content>
      {description ? <Description className="text-xs text-muted">{description}</Description> : null}
    </Switch>
  );
}

interface CheckProps {
  checked: boolean;
  onChange?: ((next: boolean) => void) | undefined;
  label: string;
  indeterminate?: boolean;
}

export function Check({ checked, onChange, label, indeterminate = false }: CheckProps) {
  return (
    <HeroCheckbox
      isSelected={checked}
      isIndeterminate={indeterminate}
      isDisabled={onChange === undefined}
      onChange={(next) => onChange?.(next)}
      aria-label={label}
      className="m-0"
    >
      <HeroCheckbox.Content>
        <HeroCheckbox.Control>
          <HeroCheckbox.Indicator />
        </HeroCheckbox.Control>
      </HeroCheckbox.Content>
    </HeroCheckbox>
  );
}

interface SegmentedProps<T extends string> {
  value: T;
  options: readonly { value: T; label: string; disabled?: boolean }[];
  onChange: (value: T) => void;
  label: string;
  className?: string | undefined;
}

// WHAT:  Segmented control = HeroUI Tabs (secondary variant) without panels.
export function Segmented<T extends string>({ value, options, onChange, label, className }: SegmentedProps<T>) {
  return (
    <Tabs
      selectedKey={value}
      onSelectionChange={(key) => {
        const next = options.find((o) => o.value === String(key));
        if (next) onChange(next.value);
      }}
      variant="secondary"
      {...(className !== undefined ? { className } : {})}
    >
      <Tabs.ListContainer>
        <Tabs.List aria-label={label}>
          {options.map((o) => (
            <Tabs.Tab key={o.value} id={o.value} isDisabled={o.disabled === true}>
              {o.label}
              <Tabs.Indicator />
            </Tabs.Tab>
          ))}
        </Tabs.List>
      </Tabs.ListContainer>
    </Tabs>
  );
}

interface DateTimeFieldProps {
  /// date → calendar + day segments; time → clock segments; datetime → both.
  kind: TemporalKind;
  /// DB text (`YYYY-MM-DD`, `HH:MM:SS`, `YYYY-MM-DD HH:MM:SS[.f][+00]`); "" = empty.
  value: string;
  onChange: (text: string) => void;
  label?: string | undefined;
  ariaLabel?: string | undefined;
  /// Fits a grid row: no label, cell-height group, tighter padding.
  compact?: boolean;
  autoFocus?: boolean;
  /// Opens the calendar (and, for timestamps, the clock) as soon as the field
  /// mounts: a grid cell edit goes straight to the picker, no trigger click.
  autoOpen?: boolean;
  isDisabled?: boolean;
  className?: string | undefined;
}

// WHAT:  HeroUI DatePicker / TimeField speaking DB text. Timestamps edit all
//        segments (date + 24h time to the second); the popover holds the
//        calendar and, for timestamps, a clock so date and time are picked in
//        one place. Times get clock segments only.
// WHERE: https://heroui.com/docs/react/components/{date-picker,time-field,calendar}
export function DateTimeField({ kind, value, onChange, label, ariaLabel, compact = false, autoFocus = false, autoOpen = false, isDisabled = false, className }: DateTimeFieldProps) {
  const groupClass = cn("font-mono", compact ? "h-full min-h-0 rounded-sm border-accent bg-background px-1 text-[12px] shadow-none" : "");
  const segmentClass = compact ? "py-0 text-[12px]" : "";
  const a11y = ariaLabel ?? label ?? kind;
  if (kind === "time") {
    return (
      <TimeField
        value={parseDbTime(value)}
        onChange={(next) => onChange(next ? formatDbTime(next) : "")}
        granularity="second"
        hourCycle={24}
        shouldForceLeadingZeros
        aria-label={a11y}
        isDisabled={isDisabled}
        autoFocus={autoFocus}
        className={cn(compact ? "h-full" : "w-full", className)}
      >
        {label ? <Label>{label}</Label> : null}
        <TimeField.Group fullWidth className={groupClass}>
          <TimeField.Prefix>
            <Icon name="clock" size={12} className="text-muted" />
          </TimeField.Prefix>
          <TimeField.Input>{(segment) => <TimeField.Segment segment={segment} className={segmentClass} />}</TimeField.Input>
        </TimeField.Group>
      </TimeField>
    );
  }
  const separator = value.includes("T") ? "T" : " ";
  return (
    <DatePicker
      value={parseDbDate(value)}
      onChange={(next) => onChange(next ? formatDbDate(next, separator) : "")}
      defaultOpen={autoOpen}
      granularity={kind === "date" ? "day" : "second"}
      hourCycle={24}
      shouldForceLeadingZeros
      hideTimeZone
      aria-label={a11y}
      isDisabled={isDisabled}
      autoFocus={autoFocus}
      className={cn(compact ? "h-full" : "w-full", className)}
    >
      {({ state }) => (
        <>
          {label ? <Label>{label}</Label> : null}
          <DateField.Group fullWidth className={groupClass}>
            <DateField.Input>{(segment) => <DateField.Segment segment={segment} className={segmentClass} />}</DateField.Input>
            <DateField.Suffix>
              <DatePicker.Trigger className={compact ? "size-5 min-w-5" : ""}>
                <DatePicker.TriggerIndicator>
                  <Icon name="calendar" size={compact ? 12 : 14} />
                </DatePicker.TriggerIndicator>
              </DatePicker.Trigger>
            </DateField.Suffix>
          </DateField.Group>
          <DatePicker.Popover className="glass-modal flex flex-col gap-3 rounded-xl">
            <Calendar aria-label={`${a11y} calendar`}>
              <Calendar.Header>
                <Calendar.YearPickerTrigger>
                  <Calendar.YearPickerTriggerHeading />
                  <Calendar.YearPickerTriggerIndicator />
                </Calendar.YearPickerTrigger>
                <Calendar.NavButton slot="previous" />
                <Calendar.NavButton slot="next" />
              </Calendar.Header>
              <Calendar.Grid>
                <Calendar.GridHeader>{(day) => <Calendar.HeaderCell>{day}</Calendar.HeaderCell>}</Calendar.GridHeader>
                <Calendar.GridBody>{(date) => <Calendar.Cell date={date} />}</Calendar.GridBody>
              </Calendar.Grid>
              <Calendar.YearPickerGrid>
                <Calendar.YearPickerGridBody>{({ year }) => <Calendar.YearPickerCell year={year} />}</Calendar.YearPickerGridBody>
              </Calendar.YearPickerGrid>
            </Calendar>
            {kind === "datetime" ? (
              <div className="flex items-center justify-between gap-3 border-t border-border/40 pt-3">
                <span className="flex items-center gap-1.5 text-xs text-muted">
                  <Icon name="clock" size={12} />
                  Time
                </span>
                <TimeField
                  aria-label={`${a11y} time`}
                  granularity="second"
                  hourCycle={24}
                  shouldForceLeadingZeros
                  hideTimeZone
                  value={state.timeValue}
                  onChange={(next) => {
                    if (next) state.setTimeValue(next);
                  }}
                >
                  <TimeField.Group variant="secondary" className="font-mono">
                    <TimeField.Input>{(segment) => <TimeField.Segment segment={segment} />}</TimeField.Input>
                  </TimeField.Group>
                </TimeField>
              </div>
            ) : null}
          </DatePicker.Popover>
        </>
      )}
    </DatePicker>
  );
}

interface NumberInputProps {
  /// null = empty.
  value: number | null;
  onChange: (next: number | null) => void;
  /// Whole numbers only (int / serial columns).
  integer?: boolean;
  label?: string | undefined;
  ariaLabel?: string | undefined;
  /// Fits a grid row: no label, no stepper buttons, cell-height input.
  compact?: boolean;
  autoFocus?: boolean;
  isDisabled?: boolean;
  className?: string | undefined;
}

// WHAT:  HeroUI NumberField without locale grouping (DB style `1234567.5`).
// WHERE: https://heroui.com/docs/react/components/number-field
export function NumberInput({ value, onChange, integer = false, label, ariaLabel, compact = false, autoFocus = false, isDisabled = false, className }: NumberInputProps) {
  return (
    <NumberField
      value={value ?? Number.NaN}
      onChange={(next) => onChange(Number.isNaN(next) ? null : next)}
      formatOptions={{ useGrouping: false, maximumFractionDigits: integer ? 0 : 20 }}
      {...(integer ? { step: 1 } : {})}
      aria-label={ariaLabel ?? label ?? "number"}
      isDisabled={isDisabled}
      autoFocus={autoFocus}
      className={cn(compact ? "h-full" : "w-full", className)}
    >
      {label ? <Label>{label}</Label> : null}
      <NumberField.Group className={cn("font-mono", compact ? "h-full min-h-0 grid-cols-1 rounded-sm border-accent bg-background shadow-none" : "")}>
        {compact ? null : <NumberField.DecrementButton />}
        <NumberField.Input className={cn("w-full tabular-nums", compact ? "h-full px-1 text-[12px]" : "")} />
        {compact ? null : <NumberField.IncrementButton />}
      </NumberField.Group>
    </NumberField>
  );
}
