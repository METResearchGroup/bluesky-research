import '@testing-library/jest-dom'

// Mock next/font/google
jest.mock('next/font/google', () => ({
  Inter: () => ({
    className: 'mock-inter-font',
  }),
}))

// Mock next/navigation
jest.mock('next/navigation', () => ({
  useRouter: () => ({
    push: jest.fn(),
    replace: jest.fn(),
    back: jest.fn(),
  }),
  useSearchParams: () => ({
    get: jest.fn(),
  }),
  usePathname: () => '/',
}))

// Global test utilities
global.fetch = jest.fn()

// Cleanup after each test
afterEach(() => {
  jest.clearAllMocks()
}) 