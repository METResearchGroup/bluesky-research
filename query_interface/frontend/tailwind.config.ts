import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: ["class"],
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        // Anthropic color palette
        anthropic: {
          dark: "#141413",
          light: "#faf9f5",
          "mid-gray": "#b0aea5",
          "light-gray": "#e8e6dc",
          orange: "#d97757",
          blue: "#6a9bcc",
          green: "#788c5d",
        },
        // shadcn/ui compatible colors using Anthropic palette
        background: "#faf9f5",
        foreground: "#141413",
        primary: {
          DEFAULT: "#d97757",
          foreground: "#faf9f5",
        },
        secondary: {
          DEFAULT: "#6a9bcc",
          foreground: "#faf9f5",
        },
        muted: {
          DEFAULT: "#e8e6dc",
          foreground: "#b0aea5",
        },
        accent: {
          DEFAULT: "#d97757",
          foreground: "#faf9f5",
        },
        border: "#b0aea5",
        input: "#b0aea5",
        ring: "#d97757",
      },
      borderRadius: {
        lg: "0.5rem",
        md: "calc(0.5rem - 2px)",
        sm: "calc(0.5rem - 4px)",
      },
    },
  },
  plugins: [],
};

export default config;

