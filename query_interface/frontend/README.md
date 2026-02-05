# Query Interface Frontend

A minimal Next.js frontend for the Query Interface that allows users to submit natural language queries and view results.

## Features

- Natural language query input
- Real-time query execution
- SQL query display
- Results table with dynamic columns
- Loading states and error handling
- Anthropic-inspired color palette

## Setup

1. Install dependencies:
```bash
npm install
```

2. Configure the API URL (optional):
Create a `.env.local` file:
```
NEXT_PUBLIC_QUERY_INTERFACE_API_URL=http://127.0.0.1:8000
```

If not set, it defaults to `http://127.0.0.1:8000`.

## Development

Run the development server:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

Make sure the backend API server is running at the configured URL.

## Build

Build for production:

```bash
npm run build
npm start
```

## Tech Stack

- **Next.js 14** - React framework with App Router
- **TypeScript** - Type safety
- **Tailwind CSS** - Styling
- **shadcn/ui** - UI components
- **Anthropic Color Palette** - Brand colors

