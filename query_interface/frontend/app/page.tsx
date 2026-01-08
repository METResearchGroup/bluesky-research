"use client";

import { useState } from "react";
import { QueryForm } from "@/components/QueryForm";
import { LoadingSpinner } from "@/components/LoadingSpinner";
import { ResultsTable } from "@/components/ResultsTable";
import { SqlDisplay } from "@/components/SqlDisplay";
import { executeQuery, type ApiErrorResponse } from "@/lib/api";
import { QueryResponse } from "@/types/api";

export default function Home() {
  const [query, setQuery] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [results, setResults] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (queryText: string) => {
    setQuery(queryText);
    setIsLoading(true);
    setError(null);
    setResults(null);

    try {
      const response = await executeQuery(queryText);
      setResults(response);
    } catch (err) {
      const apiError = err as ApiErrorResponse;
      setError(apiError.message || "An error occurred while executing the query.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <main className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8 max-w-6xl">
        <div className="space-y-8">
          {/* Header */}
          <div className="space-y-2">
            <h1 className="text-3xl font-bold text-foreground">
              Bluesky Data Explorer
            </h1>
            <p className="text-muted-foreground">
              Ask questions about Bluesky data, using plain English
            </p>
          </div>

          {/* Query Form */}
          <QueryForm onSubmit={handleSubmit} isLoading={isLoading} />

          {/* Error Display */}
          {error && (
            <div className="rounded-lg border border-red-200 bg-red-50 p-4">
              <p className="text-sm text-red-800">
                <strong>Error:</strong> {error}
              </p>
            </div>
          )}

          {/* Loading State */}
          {isLoading && <LoadingSpinner />}

          {/* Results */}
          {results && !isLoading && (
            <div className="space-y-6">
              {/* SQL Query Display */}
              <SqlDisplay sqlQuery={results.sql_query} />

              {/* Results Table */}
              <ResultsTable
                results={results.results}
                rowCount={results.row_count}
              />
            </div>
          )}
        </div>
      </div>
    </main>
  );
}

