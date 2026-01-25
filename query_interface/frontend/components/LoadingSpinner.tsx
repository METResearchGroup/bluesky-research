import { Loader2 } from "lucide-react";

export function LoadingSpinner() {
  return (
    <div className="flex items-center justify-center py-12">
      <div className="flex flex-col items-center gap-4">
        <Loader2 className="h-8 w-8 animate-spin text-anthropic-orange" />
        <p className="text-sm text-muted-foreground">Executing query...</p>
      </div>
    </div>
  );
}

