// SOT: empty-state-component
import type { ReactNode } from "react";
import { Icon, type IconName } from "@/lib/icons";

export function EmptyState({ title, body, action, icon }: { title: string; body?: string; action?: ReactNode; icon?: IconName }) {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-3 p-8 text-center">
      {icon ? (
        <span className="mb-1 flex size-12 items-center justify-center rounded-2xl glass-card text-accent shadow-lg">
          <Icon name={icon} size={20} />
        </span>
      ) : null}
      <p className="text-sm font-medium text-foreground tracking-tight">{title}</p>
      {body ? <p className="max-w-sm text-xs text-muted leading-relaxed">{body}</p> : null}
      {action ? <div className="mt-2">{action}</div> : null}
    </div>
  );
}
