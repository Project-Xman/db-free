// SOT: resizer-handle, drag-to-expand, splitters
import { useCallback, useEffect, useRef, useState } from "react";
import { cn } from "@/lib/cn";

export interface ResizerProps {
  direction?: "horizontal" | "vertical";
  /// Incremental delta (px) since the previous move event.
  onResize: (delta: number) => void;
  /// Fires once when the mouse is released after a drag.
  onDragEnd?: (() => void) | undefined;
  className?: string;
}

// WHAT:  Draggable splitter handle for expanding/collapsing sidebars,
//        inspectors, split panes and grid columns with fluid macOS-style
//        visual feedback.
// HOW:   The callbacks live in refs so a caller can hand over a fresh closure
//        every render (the grid does, per column) without the drag listeners
//        being torn down and the body cursor reset mid-drag.
export function Resizer({ direction = "horizontal", onResize, onDragEnd, className }: ResizerProps) {
  const [dragging, setDragging] = useState(false);
  const startPos = useRef(0);
  const resizeRef = useRef(onResize);
  const endRef = useRef(onDragEnd);
  useEffect(() => {
    resizeRef.current = onResize;
    endRef.current = onDragEnd;
  });

  const onMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragging(true);
      startPos.current = direction === "horizontal" ? e.clientX : e.clientY;
      document.body.style.cursor = direction === "horizontal" ? "col-resize" : "row-resize";
      document.body.style.userSelect = "none";
    },
    [direction],
  );

  useEffect(() => {
    if (!dragging) return;

    const onMouseMove = (e: MouseEvent) => {
      const currentPos = direction === "horizontal" ? e.clientX : e.clientY;
      const delta = currentPos - startPos.current;
      startPos.current = currentPos;
      resizeRef.current(delta);
    };

    const onMouseUp = () => {
      setDragging(false);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      endRef.current?.();
    };

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
  }, [dragging, direction]);

  return (
    <div
      role="separator"
      tabIndex={0}
      aria-orientation={direction}
      onMouseDown={onMouseDown}
      className={cn(
        "group relative shrink-0 transition-colors z-20 select-none",
        direction === "horizontal"
          ? "w-1.5 cursor-col-resize hover:w-1.5"
          : "h-1.5 cursor-row-resize hover:h-1.5",
        dragging ? "bg-accent/40" : "hover:bg-accent/25",
        className,
      )}
    >
      <div
        className={cn(
          "absolute bg-border/60 transition-colors group-hover:bg-accent",
          dragging && "bg-accent",
          direction === "horizontal"
            ? "top-0 bottom-0 left-[2px] w-[1px]"
            : "left-0 right-0 top-[2px] h-[1px]",
        )}
      />
    </div>
  );
}
