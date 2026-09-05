// SOT: frontend-entry, react-root, error-boundary
// Every family offered by the font picker (src/lib/fonts.ts) is bundled, so the
// choice works offline instead of pulling from Google's CDN at runtime.
import "@fontsource-variable/inter";
import "@fontsource-variable/jetbrains-mono";
import "@fontsource-variable/fira-code";
import "@fontsource-variable/source-code-pro";
import "@fontsource-variable/roboto-mono";
import "@fontsource-variable/geist-mono";
import "@fontsource-variable/geist";
import "@fontsource-variable/open-sans";
import "@fontsource-variable/source-sans-3";
import "@fontsource-variable/space-grotesk";
import "./styles/globals.css";
import { Component, StrictMode, type ErrorInfo, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";

interface BoundaryState {
  error: Error | null;
}

// WHAT:  Last-resort error surface so a render crash shows its message instead
//        of a black window.
class ErrorBoundary extends Component<{ children: ReactNode }, BoundaryState> {
  override state: BoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): BoundaryState {
    return { error };
  }

  override componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("render crash", error, info.componentStack);
  }

  override render(): ReactNode {
    if (this.state.error) {
      return (
        <div className="selectable flex h-full flex-col gap-3 bg-background p-8 text-foreground">
          <h1 className="text-lg font-semibold text-danger">DB Free hit a rendering error</h1>
          <pre className="rounded-md bg-surface p-4 font-mono text-xs whitespace-pre-wrap">{this.state.error.message}{"\n\n"}{this.state.error.stack ?? ""}</pre>
          <p className="text-xs text-muted">Reload the window (⌘R) after fixing. This screen exists so failures are never silent.</p>
        </div>
      );
    }
    return this.props.children;
  }
}

window.addEventListener("error", (event) => {
  console.error("uncaught", event.error ?? event.message);
});
window.addEventListener("unhandledrejection", (event) => {
  console.error("unhandled rejection", event.reason);
});

const container = document.getElementById("root");
if (container) {
  createRoot(container).render(
    <StrictMode>
      <ErrorBoundary>
        <App />
      </ErrorBoundary>
    </StrictMode>,
  );
}
