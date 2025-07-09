import React from 'react'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import '@testing-library/jest-dom'
import SearchForm from '../SearchForm'

// Mock Headless UI components that we expect to be used
jest.mock('@headlessui/react', () => ({
  ...jest.requireActual('@headlessui/react'),
  Popover: ({ children }: { children: React.ReactNode | ((bag: { open: boolean; close: () => void }) => React.ReactNode) }) => {
    const close = jest.fn()
    return <div>{typeof children === 'function' ? children({ open: false, close }) : children}</div>
  },
  PopoverButton: ({ children, ...props }: { children: React.ReactNode; [key: string]: unknown }) => (
    <button {...props}>{children}</button>
  ),
  PopoverPanel: ({ children, ...props }: { children: React.ReactNode; [key: string]: unknown }) => (
    <div {...props} data-testid="date-picker-dialog">{children}</div>
  ),
}))

interface SearchFormData {
  query: string
  username: string
  startDate: string
  endDate: string
  exactMatch: boolean
}

describe('SearchForm Component', () => {
  const mockOnSubmit = jest.fn()
  const user = userEvent.setup()

  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('Component Rendering & Structure', () => {
    test('renders all form inputs correctly', () => {
      const expected_result = {
        searchInput: true,
        usernameInput: true,
        startDateInput: true,
        endDateInput: true,
        exactMatchCheckbox: true,
        submitButton: true,
        formTitle: 'Search Posts'
      }

      render(<SearchForm onSubmit={mockOnSubmit} />)

      expect(screen.getByLabelText(/search query/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/username filter/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/start date/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/end date/i)).toBeInTheDocument()
      expect(screen.getByLabelText(/exact match/i)).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /search posts/i })).toBeInTheDocument()
      expect(screen.getByRole('heading', { name: expected_result.formTitle })).toBeInTheDocument()
    })

    test('uses native date input components for better UX', () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      // Expect native date input components 
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      expect(startDatePicker).toBeInTheDocument()
      expect(endDatePicker).toBeInTheDocument()
      expect(startDatePicker).toHaveAttribute('type', 'date')
      expect(endDatePicker).toHaveAttribute('type', 'date')
    })

    test('applies proper styling and layout classes', () => {
      const expected_result = {
        containerClasses: ['bg-white', 'rounded-2xl', 'shadow-sm', 'border', 'border-gray-200', 'p-6'],
        formClasses: ['space-y-4']
      }

      const { container } = render(<SearchForm onSubmit={mockOnSubmit} />)
      const formContainer = container.firstChild as HTMLElement
      const form = screen.getByRole('form') || container.querySelector('form')

      expected_result.containerClasses.forEach(cls => {
        expect(formContainer).toHaveClass(cls)
      })
      
      expected_result.formClasses.forEach(cls => {
        expect(form).toHaveClass(cls)
      })
    })
  })

  describe('Form Validation', () => {
    test('shows error when search query is empty and form is submitted', async () => {
      const expected_result = {
        errorMessage: 'Search query is required',
        submitCount: 0
      }

      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const submitButton = screen.getByRole('button', { name: /search posts/i })
      await user.click(submitButton)

      expect(screen.getByText(expected_result.errorMessage)).toBeInTheDocument()
      expect(mockOnSubmit).toHaveBeenCalledTimes(expected_result.submitCount)
    })

    test('validates username format with clear error messages', async () => {
      const expected_result = {
        invalidUsername: 'invalid user!@#',
        errorMessage: 'Username can only contain letters, numbers, dots, hyphens, and underscores',
        validUsername: 'valid_user.name-123'
      }

      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const usernameInput = screen.getByLabelText(/username filter/i)
      const searchInput = screen.getByLabelText(/search query/i)
      
      // Test invalid username
      await user.type(usernameInput, expected_result.invalidUsername)
      await user.type(searchInput, 'test query')
      await user.click(screen.getByRole('button', { name: /search posts/i }))

      expect(screen.getByText(expected_result.errorMessage)).toBeInTheDocument()

      // Clear and test valid username
      await user.clear(usernameInput)
      await user.type(usernameInput, expected_result.validUsername)
      await user.click(screen.getByRole('button', { name: /search posts/i }))

      await waitFor(() => {
        expect(screen.queryByText(expected_result.errorMessage)).not.toBeInTheDocument()
      })
    })

    test('validates that end date is after start date', async () => {
      const expected_result = {
        startDate: '2024-01-15',
        invalidEndDate: '2024-01-10',
        errorMessage: 'End date must be after start date',
        validEndDate: '2024-01-20'
      }

      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      const searchInput = screen.getByLabelText(/search query/i)
      
      await user.type(searchInput, 'test query')
      
      // Set start date directly on native input
      await user.type(startDatePicker, expected_result.startDate)
      
      // Set invalid end date (before start date) 
      await user.type(endDatePicker, expected_result.invalidEndDate)
      
      await user.click(screen.getByRole('button', { name: /search posts/i }))

      expect(screen.getByText(expected_result.errorMessage)).toBeInTheDocument()
    })
  })

  describe('Form Submission & Loading States', () => {
    test('prevents duplicate submissions when loading', async () => {
      const expected_result = {
        submitCount: 1,
        loadingText: 'Searching...',
        disabledState: true
      }

      render(<SearchForm onSubmit={mockOnSubmit} isLoading={true} />)
      
      const submitButton = screen.getByRole('button', { name: /searching/i })
      
      expect(submitButton).toBeDisabled()
      expect(submitButton).toHaveAttribute('disabled')
      expect(screen.getByText(expected_result.loadingText)).toBeInTheDocument()
      
      // Try to click disabled button
      await user.click(submitButton)
      expect(mockOnSubmit).toHaveBeenCalledTimes(0)
    })

    test('submits form with correct data structure', async () => {
      const expected_result: SearchFormData = {
        query: 'test search query',
        username: 'testuser',
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        exactMatch: true
      }

      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      // Fill out all form fields
      await user.type(screen.getByLabelText(/search query/i), expected_result.query)
      await user.type(screen.getByLabelText(/username filter/i), expected_result.username)
      
      // Use native date inputs
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      await user.type(startDatePicker, expected_result.startDate)
      await user.type(endDatePicker, expected_result.endDate)
      
      await user.click(screen.getByLabelText(/exact match/i))
      
      await user.click(screen.getByRole('button', { name: /search posts/i }))

      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalledWith(expected_result)
      })
    })

    test('handles form submission errors gracefully', async () => {
      const expected_result = {
        error: new Error('Network error'),
        consoleErrorCalls: 1
      }

      const consoleSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
      mockOnSubmit.mockRejectedValueOnce(expected_result.error)

      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      await user.type(screen.getByLabelText(/search query/i), 'test')
      await user.click(screen.getByRole('button', { name: /search posts/i }))

      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledTimes(expected_result.consoleErrorCalls)
        expect(consoleSpy).toHaveBeenCalledWith('Search failed:', expected_result.error)
      })

      consoleSpy.mockRestore()
    })
  })

  describe('Accessibility Compliance (WCAG AA)', () => {
    test('provides proper ARIA labels and descriptions', () => {
      const expected_result = {
        requiredFieldIndicator: '*'
      }

      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const searchInput = screen.getByLabelText(/search query/i)
      const usernameInput = screen.getByLabelText(/username filter/i)
      const exactMatchCheckbox = screen.getByLabelText(/exact match/i)
      
      expect(searchInput).toHaveAttribute('aria-required', 'true')
      expect(searchInput).toHaveAccessibleName()
      expect(usernameInput).toHaveAccessibleName()
      expect(exactMatchCheckbox).toHaveAccessibleName()
      
      // Check for required field indicator in label
      expect(screen.getByText(expected_result.requiredFieldIndicator)).toBeInTheDocument()
    })

    test('announces errors to screen readers with proper roles', async () => {
      const expected_result = {
        errorRole: 'alert',
        ariaLive: 'polite'
      }

      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      await user.click(screen.getByRole('button', { name: /search posts/i }))

      const errorMessage = screen.getByText(/search query is required/i)
      expect(errorMessage).toHaveAttribute('role', expected_result.errorRole)
    })

    test('supports keyboard navigation throughout form', async () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      // Test Tab navigation
      const searchInput = screen.getByLabelText(/search query/i)
      const usernameInput = screen.getByLabelText(/username filter/i)
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      const exactMatchCheckbox = screen.getByLabelText(/exact match/i)
      const submitButton = screen.getByRole('button', { name: /search posts/i })

      searchInput.focus()
      expect(searchInput).toHaveFocus()

      await user.tab()
      expect(usernameInput).toHaveFocus()

      await user.tab()
      expect(startDatePicker).toHaveFocus()

      await user.tab()
      expect(endDatePicker).toHaveFocus()

      await user.tab()
      expect(exactMatchCheckbox).toHaveFocus()

      await user.tab()
      expect(submitButton).toHaveFocus()
    })

    test('date inputs support keyboard navigation and input', async () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      await act(async () => {
        startDatePicker.focus()
      })

      // Test keyboard input on native date inputs
      await user.type(startDatePicker, '2024-01-01')
      expect(startDatePicker).toHaveValue('2024-01-01')

      // Test tab navigation to next date input
      await user.tab()
      expect(endDatePicker).toHaveFocus()

      await user.type(endDatePicker, '2024-01-31')
      expect(endDatePicker).toHaveValue('2024-01-31')
    })
  })

  describe('Responsive Design & UX', () => {
    test('adapts layout for mobile and desktop viewports', () => {
      const expected_result = {
        mobileClasses: ['w-full'],
        desktopClasses: ['md:w-auto', 'md:grid-cols-2']
      }

      const { container } = render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const submitButton = screen.getByRole('button', { name: /search posts/i })
      const dateContainer = container.querySelector('.grid')

      expect(submitButton).toHaveClass(expected_result.mobileClasses[0])
      expect(submitButton).toHaveClass(expected_result.desktopClasses[0])
      expect(dateContainer).toHaveClass(expected_result.desktopClasses[1])
    })

    test('shows appropriate loading spinner and text during submission', () => {
      const expected_result = {
        loadingText: 'Searching...',
        spinnerClass: 'animate-spin',
        iconPresent: true
      }

      render(<SearchForm onSubmit={mockOnSubmit} isLoading={true} />)
      
      expect(screen.getByText(expected_result.loadingText)).toBeInTheDocument()
      
      const spinner = document.querySelector('.animate-spin')
      expect(spinner).toBeInTheDocument()
      expect(spinner).toHaveClass(expected_result.spinnerClass)
    })

    test('displays search icon when not loading', () => {
      const expected_result = {
        buttonText: 'Search Posts',
        iconTestId: 'magnifying-glass-icon'
      }

      render(<SearchForm onSubmit={mockOnSubmit} isLoading={false} />)
      
      const button = screen.getByRole('button', { name: expected_result.buttonText })
      expect(button).toBeInTheDocument()
      
      // Check for Heroicon presence (MagnifyingGlassIcon)
      const icon = button.querySelector('svg')
      expect(icon).toBeInTheDocument()
    })
  })

  describe('Native Date Input Integration', () => {
    test('uses native date inputs for better accessibility and UX', () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      expect(startDatePicker).toHaveAttribute('type', 'date')
      expect(endDatePicker).toHaveAttribute('type', 'date')
      expect(startDatePicker).toHaveAttribute('id', 'startDate')
      expect(endDatePicker).toHaveAttribute('id', 'endDate')
    })

    test('date inputs handle focus and form validation properly', async () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      await act(async () => {
        startDatePicker.focus()
      })
      
      expect(startDatePicker).toHaveFocus()
      
      // Test that date inputs integrate with form validation
      await user.type(startDatePicker, '2024-01-15')
      await user.type(endDatePicker, '2024-01-10') // Invalid: before start date
      
      expect(startDatePicker).toHaveValue('2024-01-15')
      expect(endDatePicker).toHaveValue('2024-01-10')
    })
  })
}) 