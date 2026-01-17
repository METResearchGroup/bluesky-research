/**
 * TypeScript types matching the backend API schemas.
 */

export interface QueryRequest {
  query: string;
}

export interface QueryResponse {
  sql_query: string;
  original_query: string;
  results: Record<string, unknown>[];
  row_count: number;
}

export interface ApiError {
  detail: string;
}

