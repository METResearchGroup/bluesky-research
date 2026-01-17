"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp, Code } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface SqlDisplayProps {
  sqlQuery: string;
}

export function SqlDisplay({ sqlQuery }: SqlDisplayProps) {
  const [isExpanded, setIsExpanded] = useState(true);

  return (
    <div className="rounded-lg border border-border bg-muted/30">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex w-full items-center justify-between px-4 py-3 text-left hover:bg-muted/50 transition-colors"
      >
        <div className="flex items-center gap-2">
          <Code className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Generated SQL</span>
        </div>
        {isExpanded ? (
          <ChevronUp className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        )}
      </button>
      {isExpanded && (
        <div className="px-4 pb-4">
          <pre className="overflow-x-auto rounded-md bg-background p-4 text-xs font-mono text-foreground border border-border">
            <code>{sqlQuery}</code>
          </pre>
        </div>
      )}
    </div>
  );
}

