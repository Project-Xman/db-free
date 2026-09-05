// SOT: context-menu, right-click-menu, menu-at-pointer
import { useCallback, useState, type MouseEvent as ReactMouseEvent, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { Button, Dropdown, Label } from "@heroui/react";
import { Icon, type IconName } from "@/lib/icons";
import { cn } from "@/lib/cn";

export interface MenuEntry {
  id: string;
  label: string;
  icon?: IconName;
  /// Rendered in the danger colour (close all, delete…).
  danger?: boolean;
  disabled?: boolean;
  /// Consecutive entries sharing a group render inside one separated section.
  group?: string;
}

interface MenuState {
  x: number;
  y: number;
  entries: readonly MenuEntry[];
  onAction: (id: string) => void;
}

export interface ContextMenu {
  /// Call from an `onContextMenu` handler: opens the menu at the pointer.
  open: (event: ReactMouseEvent, entries: readonly MenuEntry[], onAction: (id: string) => void) => void;
  /// Render once inside the component that owns the menu.
  node: ReactNode;
}

// WHAT:  Right-click menu anchored to the pointer, built on the HeroUI Dropdown
//        so it inherits menu keyboard handling, focus return and the glass
//        popover styling.
// WHY:   HeroUI v3 has no context-menu trigger: MenuTrigger anchors to a real
//        element. A zero-size fixed anchor placed at the click point gives the
//        popover something to position against without touching the caller's
//        layout — important in the data grid, where cells are absolutely
//        positioned inside a virtualized track and cannot be wrapped.
// HOW:   `open(event, entries, onAction)` stores the pointer position and the
//        entries; the menu closes on action, Escape or an outside press.
// WHERE: src/features/shell/TabBar.tsx, src/features/grid/DataGrid.tsx
export function useContextMenu(): ContextMenu {
  const [state, setState] = useState<MenuState | null>(null);

  const open = useCallback((event: ReactMouseEvent, entries: readonly MenuEntry[], onAction: (id: string) => void) => {
    if (entries.length === 0) return;
    event.preventDefault();
    event.stopPropagation();
    setState({ x: event.clientX, y: event.clientY, entries, onAction });
  }, []);

  // Portalled to <body>: a `position: fixed` anchor is positioned against the
  // nearest ancestor that has a mask, filter or transform — and the scroll
  // shadows the grid and tab bar use are mask-image, which made the menu open
  // an entire container away from the pointer.
  const node =
    state === null
      ? null
      : createPortal(
      <Dropdown
        isOpen
        onOpenChange={(next) => {
          if (!next) setState(null);
        }}
      >
        {/* The anchor is positioned by a plain div: HeroUI's Button carries its
            own position utility, which would win over a `fixed` class here and
            leave the popover anchored wherever the button landed in flow. */}
        <div className="pointer-events-none fixed z-50 size-px" style={{ left: state.x, top: state.y }}>
          <Button aria-label="Context menu" excludeFromTabOrder className="size-px min-w-0 border-0 bg-transparent p-0 opacity-0" />
        </div>
        <Dropdown.Popover placement="bottom start" className="min-w-52 glass-modal rounded-xl">
          <Dropdown.Menu
            onAction={(key) => {
              state.onAction(String(key));
              setState(null);
            }}
          >
            {groupEntries(state.entries).map((group, index) => (
              <Dropdown.Section key={group[0]?.group ?? String(index)}>
                {group.map((entry) => (
                  <Dropdown.Item key={entry.id} id={entry.id} textValue={entry.label} isDisabled={entry.disabled ?? false}>
                    {entry.icon ? <Icon name={entry.icon} size={13} className={cn("shrink-0", entry.danger ? "text-danger" : "text-muted")} /> : null}
                    <Label className={cn("ml-2 truncate", entry.danger ? "text-danger" : "")}>{entry.label}</Label>
                  </Dropdown.Item>
                ))}
              </Dropdown.Section>
            ))}
          </Dropdown.Menu>
        </Dropdown.Popover>
      </Dropdown>,
      document.body,
    );

  return { open, node };
}

/// Splits the flat list into runs of the same `group`, so callers describe
/// sections by tagging entries instead of nesting arrays.
function groupEntries(entries: readonly MenuEntry[]): MenuEntry[][] {
  const groups: MenuEntry[][] = [];
  for (const entry of entries) {
    const last = groups[groups.length - 1];
    if (last && (last[0]?.group ?? "") === (entry.group ?? "")) last.push(entry);
    else groups.push([entry]);
  }
  return groups;
}
