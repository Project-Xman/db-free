// SOT: kbd-component, platform-modifier
import { Kbd as HeroKbd } from "@heroui/react";

const IS_MAC = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.userAgent);

export function isMac(): boolean {
  return IS_MAC;
}

export function RunShortcut() {
  return (
    <HeroKbd className="text-[10px]">
      <HeroKbd.Abbr keyValue={IS_MAC ? "command" : "ctrl"} />
      <HeroKbd.Content>↵</HeroKbd.Content>
    </HeroKbd>
  );
}
