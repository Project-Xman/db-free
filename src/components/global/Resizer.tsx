// SOT: resizer-handle, drag-to-expand, splitters
import { useCallback, useEffect, useRef, useState, type PointerEvent as ReactPointerEvent } from "react";
import { cn } from "@/lib/cn";

export interface ResizerProps {
  direction?: "horizontal" | "vertical";
  /// Incremental delta (px) since the previous move event.
  onResize: (delta: number) => void;
  /// Fires once when the pointer is released after a drag.
  onDragEnd?: (() => void) | undefined;
  className?: string;
}

// WHAT:  Draggable splitter handle for expanding/collapsing sidebars,
//        inspectors, split panes and grid columns with fluid macOS-style
//        visual feedback.
// HOW:   Pointer events with pointer capture, not mouse events on `window`:
//        the grid re-renders on every delta (each column width is state), and
//        capture keeps the whole drag bound to this element even while the
//        surrounding virtualized header re-renders under the cursor. Touch and
//        pen drags come free; `touch-action: none` stops the pane scrolling
//        instead of resizing. The callbacks live in refs so a caller can hand
//        over a fresh closure every render (the grid does, per column) without
//        the drag being torn down and the body cursor reset mid-drag.
export function Resizer({ direction = "horizontal", onResize, onDragEnd, className }: ResizerProps) {
  const [dragging, setDragging] = useState(false);
  const startPos = useRef(0);
  const resizeRef = useRef(onResize);
  const endRef = useRef(onDragEnd);
  useEffect(() => {
    resizeRef.current = onResize;
    endRef.current = onDragEnd;
  });

  // The body cursor survives a mid-drag unmount (a column scrolling out of the
  // virtualized window) only if something clears it.
  useEffect(
    () => () => {
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    },
    [],
  );

  const begin = useCallback(
    (e: ReactPointerEvent<HTMLDivElement>) => {
      e.preventDefault();
      e.stopPropagation();
      e.currentTarget.setPointerCapture(e.pointerId);
      startPos.current = direction === "horizontal" ? e.clientX : e.clientY;
      setDragging(true);
      document.body.style.cursor = direction === "horizontal" ? "col-resize" : "row-resize";
      document.body.style.userSelect = "none";
    },
    [direction],
  );

  const move = useCallback(
    (e: ReactPointerEvent<HTMLDivElement>) => {
      if (!dragging) return;
      const pos = direction === "horizontal" ? e.clientX : e.clientY;
      const delta = pos - startPos.current;
      if (delta === 0) return;
      startPos.current = pos;
      resizeRef.current(delta);
    },
    [dragging, direction],
  );

  const end = useCallback(
    (e: ReactPointerEvent<HTMLDivElement>) => {
      if (!dragging) return;
      if (e.currentTarget.hasPointerCapture(e.pointerId)) e.currentTarget.releasePointerCapture(e.pointerId);
      setDragging(false);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      endRef.current?.();
    },
    [dragging],
  );

  return (
    <div
      role="separator"
      tabIndex={0}
      aria-orientation={direction}
      onPointerDown={begin}
      onPointerMove={move}
      onPointerUp={end}
      onPointerCancel={end}
      className={cn(
        "group relative shrink-0 touch-none select-none transition-colors z-20",
        direction === "horizontal" ? "w-1.5 cursor-col-resize" : "h-1.5 cursor-row-resize",
        dragging ? "bg-accent/40" : "hover:bg-accent/25",
        className,
      )}
    >
      <div
        className={cn(
          "pointer-events-none absolute bg-border/60 transition-colors group-hover:bg-accent",
          dragging && "bg-accent",
          direction === "horizontal" ? "top-0 bottom-0 left-[2px] w-[1px]" : "left-0 right-0 top-[2px] h-[1px]",
        )}
      />
    </div>
  );
}
