"use client";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface ResultsTableProps {
  results: Record<string, unknown>[];
  rowCount: number;
}

export function ResultsTable({ results, rowCount }: ResultsTableProps) {
  if (results.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-background p-8 text-center">
        <p className="text-muted-foreground">No results found.</p>
      </div>
    );
  }

  // Get column names from first result
  const columns = Object.keys(results[0]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-foreground">
          Results ({rowCount} {rowCount === 1 ? "row" : "rows"})
        </h3>
        {rowCount >= 10 && (
          <p className="text-sm text-muted-foreground">
            Note: Results are limited to 10 rows.
          </p>
        )}
      </div>
      <div className="rounded-lg border border-border overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((column) => (
                <TableHead key={column} className="font-semibold text-foreground">
                  {column}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {results.map((row, rowIndex) => (
              <TableRow key={rowIndex}>
                {columns.map((column) => (
                  <TableCell key={column} className="text-foreground">
                    {row[column] !== null && row[column] !== undefined
                      ? String(row[column])
                      : ""}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

