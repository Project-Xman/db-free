// SOT: icon-button, tooltip-button
import { Button, Tooltip } from "@heroui/react";
import { Icon, type IconName } from "@/lib/icons";
import { cn } from "@/lib/cn";

interface IconButtonProps {
  icon: IconName;
  label: string;
  onPress?: () => void;
  isDisabled?: boolean;
  active?: boolean;
  size?: number;
  className?: string;
}

// WHAT:  HeroUI icon-only button with a tooltip carrying the accessible label.
export function IconButton({ icon, label, onPress, isDisabled = false, active = false, size = 15, className }: IconButtonProps) {
  return (
    <Tooltip delay={500}>
      <Button
        isIconOnly
        aria-label={label}
        size="sm"
        variant={active ? "secondary" : "ghost"}
        isDisabled={isDisabled}
        {...(onPress ? { onPress } : {})}
        className={cn(
          "size-7 min-w-7 rounded-lg liquid-hover",
          active ? "glass-pill text-accent" : "text-muted hover:bg-surface-secondary/70 hover:text-foreground",
          className,
        )}
      >
        <Icon name={icon} size={size} />
      </Button>
      <Tooltip.Content>{label}</Tooltip.Content>
    </Tooltip>
  );
}
