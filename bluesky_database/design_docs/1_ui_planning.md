# Product Requirements Document (PRD)

## Title
**Bluesky Post Explorer — Frontend UI**

## Author
Mark Torres

## Date
2025-07-07

---

## 1. Overview

This document defines the frontend user interface for the **Bluesky Post Explorer**, a tool for querying and exploring Bluesky posts based on simple filters (e.g., keywords, usernames, date ranges). The frontend will be implemented using **Next.js**, styled with **Tailwind CSS**, and hosted on **Vercel**.

The purpose of this document is to guide a frontend engineer in building the complete UI layer, including interactions, layout, styling, and expected API contracts. All backend development (including endpoints and data responses) will be handled separately.

---

## 2. Objectives

- Allow users to search for posts by **text or hashtag**
- Allow users to filter by:
  - **Username**
  - **Date range**
  - **Exact match**
- Show a **preview of results** (first 10 rows only)
- Allow users to **export results as a CSV file**
- Display a **"Coming Soon" section** for future filters (e.g., political, outrage, toxicity)
- Provide a clean, modern, and responsive user interface

---

## 3. Scope

✅ Implement:
- Page layout and design
- Frontend state and interactions
- Form inputs for filters
- Preview table with dummy/mock data
- Export CSV button
- Coming soon section
- All visual styling and layout

🚫 Do not implement:
- Backend fetching logic
- Actual API integrations
- Data processing logic

---

## 4. Pages

### `/` — Main Search Interface

#### Components

##### 1. Header
- Fixed top navigation bar
- Elements:
  - App name: **Bluesky Post Explorer**
  - (Optional) Logo or icon
  - Placeholder for settings icon

##### 2. Search Panel

| Field             | Type         | Notes                            |
|------------------|--------------|----------------------------------|
| Search Input      | Text field   | Placeholder: `Search by text or hashtag` |
| Username Filter   | Text field   | Placeholder: `Filter by username` |
| Date Range        | Date range picker | Selectable start and end dates |
| Exact Match       | Toggle switch | Label: `Exact match only` |
| Submit Button     | Button       | Text: `Search Posts` |

- Layout:
  - Mobile: stacked vertically
  - Desktop: horizontally aligned

##### 3. Results Preview Table

- Displays up to 10 results (mocked initially)
- Columns:
  - Timestamp
  - Username
  - Post preview (truncate to ~140 characters)
  - (Optional) View link
- Below table: Button `Export as CSV`

##### 4. Coming Soon Panel

- Grayed-out section for upcoming filters:
  - Political content
  - Outrage detection
  - Toxicity
- Each option:
  - Toggle (disabled)
  - Tooltip on hover: `Coming soon!`

##### 5. Footer

- GitHub link (placeholder)
- Contact email (optional)
- API docs link (placeholder)

---

## 5. User Flow

1. User arrives on landing page
2. Enters a search query and optional filters
3. Clicks “Search Posts”
4. Mocked results are rendered in a preview table
5. User can click “Export as CSV” to download a file
6. User sees placeholders for additional upcoming filters

---

## 6. Visual Design

### Typography
- Font: `Inter` or `Open Sans`
- Use Tailwind's `text-sm`, `text-base`, `text-lg` as needed

### Colors
- Primary: `blue-500` for buttons and active states
- Background: `gray-50` (`#F9FAFB`)
- Table borders: `gray-200`
- Text: `gray-700`

### Layout
- Card-style grouping for search and preview
- Use `rounded-2xl` and `shadow-sm` for modern look
- Consistent spacing (Tailwind `space-y-4`, `p-6`, etc.)

### Responsive Design
- Stack inputs vertically on small screens
- Collapsible “Coming Soon” section on mobile

---

## 7. Required API Endpoints (Backend to Implement)

> These endpoints will be mocked for now, but the UI must be wired to expect and handle the following:

### `GET /search`
- Parameters:
  - `text`: string
  - `username`: string (optional)
  - `start`: ISO date
  - `end`: ISO date
  - `exact`: boolean
- Response:
```json
{
  "results": [
    {
      "timestamp": "2025-07-06T14:03:00Z",
      "username": "exampleuser",
      "text": "This is a sample post about #example"
    },
    ...
  ],
  "meta": {
    "count": 10
  }
}
