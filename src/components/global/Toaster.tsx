// SOT: toaster, toast-region, notifications-ui
import { Toast } from "@heroui/react";

// WHAT:  Single toast region for the app (HeroUI Toast, sonner-style stack).
//        Fire toasts through the workspace store's showInfo / showError.
export function Toaster() {
  return <Toast.Provider placement="bottom end" maxVisibleToasts={3} width={380} />;
}
