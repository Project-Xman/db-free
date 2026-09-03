// SOT: form-fields, text-field, select-field, toggle-field, segmented-control, checkbox-field
import type { ReactNode } from "react";
import {
  Checkbox as HeroCheckbox,
  Description,
  Input,
  InputGroup,
  Label,
  ListBox,
  Select,
  Switch,
  Tabs,
  TextField,
} from "@heroui/react";
import { cn } from "@/lib/cn";
import { Icon, type IconName } from "@/lib/icons";

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
}

export function Field({ label, value, onChange, placeholder, type = "text", description, optional = false, isDisabled = false, autoFocus = false, className, suffix, mono = false }: FieldProps) {
  const inputClass = cn("w-full", mono ? "font-mono" : "");
  const placeholderProps = placeholder !== undefined ? { placeholder } : {};
  return (
    <TextField value={value} onChange={onChange} type={type} isDisabled={isDisabled} className={cn("w-full", className)} autoFocus={autoFocus}>
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
        {leading ? <Icon name={leading} size={plain ? 12 : 14} className="shrink-0 text-accent" /> : null}
        <Select.Value className={cn("min-w-0 truncate text-left leading-normal", plain ? "max-w-[150px]" : "flex-1")} />
        <Icon name="chevron-down" size={12} className="shrink-0 text-muted" />
      </Select.Trigger>
      <Select.Popover>
        <ListBox>
          {options.map((o) => (
            <ListBox.Item key={o.value} id={o.value} textValue={o.label}>
              {o.icon ? <Icon name={o.icon} size={14} className="mr-2 text-muted" /> : null}
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
  return (
    <div className={cn("flex gap-3", description ? "items-start" : "items-center")}>
      <Switch isSelected={checked} onChange={onChange} aria-label={label.length > 0 ? label : "toggle"} className={description ? "mt-0.5" : ""}>
        {/* Switch.Content is the interactive react-aria SwitchButton; Control alone renders a dead pill. */}
        <Switch.Content>
          <Switch.Control>
            <Switch.Thumb />
          </Switch.Control>
        </Switch.Content>
      </Switch>
      {label.length > 0 || description ? (
        <button type="button" onClick={() => onChange(!checked)} className="min-w-0 text-left">
          {label.length > 0 ? <span className="block text-[13px] text-foreground">{label}</span> : null}
          {description ? <span className="block text-xs text-muted">{description}</span> : null}
        </button>
      ) : null}
    </div>
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
