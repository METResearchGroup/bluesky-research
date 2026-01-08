"use client";

import { useState, FormEvent } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Search } from "lucide-react";

interface QueryFormProps {
  onSubmit: (query: string) => void;
  isLoading: boolean;
}

export function QueryForm({ onSubmit, isLoading }: QueryFormProps) {
  const [query, setQuery] = useState("");

  const handleSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (query.trim() && !isLoading) {
      onSubmit(query.trim());
    }
  };

  return (
    <form onSubmit={handleSubmit} className="w-full space-y-4">
      <div className="flex gap-2">
        <div className="relative flex-1">
          <Input
            type="text"
            placeholder="Enter your natural language query..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            disabled={isLoading}
            className="w-full pr-10"
          />
          <Search className="absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        </div>
        <Button type="submit" disabled={isLoading || !query.trim()}>
          {isLoading ? "Querying..." : "Submit"}
        </Button>
      </div>
    </form>
  );
}

