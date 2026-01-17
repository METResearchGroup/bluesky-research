import { QueryRequest, QueryResponse, ApiError } from "@/types/api";

const API_URL =
  process.env.NEXT_PUBLIC_QUERY_INTERFACE_API_URL || "http://127.0.0.1:8000";

export interface ApiErrorResponse {
  status: number;
  message: string;
}

/**
 * Execute a natural language query against the backend API.
 *
 * @param query - The natural language query string
 * @returns Promise resolving to the query response
 * @throws {ApiErrorResponse} If the API request fails
 */
export async function executeQuery(
  query: string
): Promise<QueryResponse> {
  try {
    const response = await fetch(`${API_URL}/query`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query } as QueryRequest),
    });

    if (!response.ok) {
      let errorMessage = "An error occurred";
      try {
        const errorData: ApiError = await response.json();
        errorMessage = errorData.detail || errorMessage;
      } catch {
        errorMessage = `HTTP ${response.status}: ${response.statusText}`;
      }

      throw {
        status: response.status,
        message: errorMessage,
      } as ApiErrorResponse;
    }

    const data: QueryResponse = await response.json();
    return data;
  } catch (error) {
    // Handle network errors
    if (error instanceof TypeError && error.message.includes("fetch")) {
      throw {
        status: 0,
        message: "Could not connect to API server. Make sure the backend is running.",
      } as ApiErrorResponse;
    }

    // Re-throw API errors
    if (error && typeof error === "object" && "status" in error) {
      throw error;
    }

    // Unknown error
    throw {
      status: 500,
      message: error instanceof Error ? error.message : "An unexpected error occurred",
    } as ApiErrorResponse;
  }
}

