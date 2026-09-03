// SOT: empty-state-component
import type { ReactNode } from "react";
import { Icon, type IconName } from "@/lib/icons";

export function EmptyState({ title, body, action, icon }: { title: string; body?: string; action?: ReactNode; icon?: IconName }) {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-2 p-8 text-center">
      {icon ? (
        <span className="mb-1 flex size-10 items-center justify-center rounded-xl bg-surface-secondary text-muted">
          <Icon name={icon} size={18} />
        </span>
      ) : null}
      <p className="text-sm font-medium text-foreground">{title}</p>
      {body ? <p className="max-w-sm text-xs text-muted">{body}</p> : null}
      {action ? <div className="mt-2">{action}</div> : null}
    </div>
  );
}
