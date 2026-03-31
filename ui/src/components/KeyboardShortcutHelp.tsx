import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { SHORTCUTS_MAP } from "@/hooks/useKeyboardShortcuts";

interface KeyboardShortcutHelpProps {
  open: boolean;
  onClose: () => void;
}

const navigationShortcuts = SHORTCUTS_MAP.filter((s) => s.route !== null);
const actionShortcuts = SHORTCUTS_MAP.filter((s) => s.route === null);

function ShortcutRow({ keys, description }: { keys: string; description: string }) {
  const badges = keys.split(" ");
  return (
    <div className="flex items-center justify-between py-1.5">
      <span className="text-sm text-muted-foreground">{description}</span>
      <div className="flex items-center gap-1">
        {badges.map((badge, i) => (
          <kbd
            key={i}
            className="bg-muted rounded px-1.5 py-0.5 text-xs font-mono"
          >
            {badge}
          </kbd>
        ))}
      </div>
    </div>
  );
}

export function KeyboardShortcutHelp({ open, onClose }: KeyboardShortcutHelpProps) {
  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Keyboard Shortcuts</DialogTitle>
        </DialogHeader>

        <div className="space-y-4 mt-2">
          {/* Navigation group */}
          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-2">
              Navigation
            </h4>
            <div className="space-y-0.5">
              {navigationShortcuts.map((s) => (
                <ShortcutRow key={s.keys} keys={s.keys} description={s.description} />
              ))}
            </div>
          </div>

          {/* Actions group */}
          <div>
            <h4 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-2">
              Actions
            </h4>
            <div className="space-y-0.5">
              {actionShortcuts.map((s) => (
                <ShortcutRow key={s.keys} keys={s.keys} description={s.description} />
              ))}
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
