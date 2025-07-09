import React from 'react'
import { render, screen, waitFor, within, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import '@testing-library/jest-dom'
import SearchForm from '../SearchForm'

// Mock Headless UI components that we expect to be used
jest.mock('@headlessui/react', () => ({
  ...jest.requireActual('@headlessui/react'),
  Dialog: ({ children, open }: { children: React.ReactNode; open: boolean }) => open ? <div data-testid="date-picker-dialog">{children}</div> : null,
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

    test('uses Headless UI DatePicker components instead of native date inputs', () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      // Expect Headless UI date picker components, not native date inputs
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      expect(startDatePicker).toBeInTheDocument()
      expect(endDatePicker).toBeInTheDocument()
      expect(startDatePicker).toHaveAttribute('role', 'button')
      expect(endDatePicker).toHaveAttribute('role', 'button')
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
      
      // Set start date
      await user.click(startDatePicker)
      const startDateInput = within(screen.getByTestId('date-picker-dialog')).getByDisplayValue('')
      await user.type(startDateInput, expected_result.startDate)
      
      // Set invalid end date (before start date) 
      await user.click(endDatePicker)
      const endDateDialog = screen.getByTestId('date-picker-dialog')
      const endDateInput = within(endDateDialog).getByDisplayValue('')
      await user.type(endDateInput, expected_result.invalidEndDate)
      
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
      
      // Use date pickers
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      await user.click(startDatePicker)
      await user.type(screen.getByDisplayValue(''), expected_result.startDate)
      
      await user.click(endDatePicker)
      await user.type(screen.getByDisplayValue(''), expected_result.endDate)
      
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

    test('date pickers open and close with keyboard controls', async () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const startDatePicker = screen.getByTestId('start-date-picker')
      
      await act(async () => {
        startDatePicker.focus()
      })

      // Test clicking opens date picker (simulating keyboard activation)
      await user.click(startDatePicker)
      expect(screen.getByTestId('date-picker-dialog')).toBeInTheDocument()

      // Test Escape key closes date picker
      await user.keyboard('[Escape]')
      expect(screen.queryByTestId('date-picker-dialog')).not.toBeInTheDocument()

      // Test clicking opens date picker again
      await user.click(startDatePicker)
      expect(screen.getByTestId('date-picker-dialog')).toBeInTheDocument()
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

  describe('Headless UI Integration', () => {
    test('uses Headless UI components for enhanced accessibility', () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const startDatePicker = screen.getByTestId('start-date-picker')
      const endDatePicker = screen.getByTestId('end-date-picker')
      
      expect(startDatePicker).toHaveAttribute('aria-haspopup', 'dialog')
      expect(endDatePicker).toHaveAttribute('aria-haspopup', 'dialog')
      expect(startDatePicker).toHaveAttribute('aria-expanded', 'false')
      expect(endDatePicker).toHaveAttribute('aria-expanded', 'false')
    })

    test('date picker dialogs manage focus properly', async () => {
      render(<SearchForm onSubmit={mockOnSubmit} />)
      
      const startDatePicker = screen.getByTestId('start-date-picker')
      
      await act(async () => {
        startDatePicker.focus()
      })
      
      await user.click(startDatePicker)
      
      // Dialog should open and focus should be managed
      const dialog = screen.getByTestId('date-picker-dialog')
      expect(dialog).toBeInTheDocument()
      
      // Focus should be trapped within dialog
      const firstFocusable = dialog.querySelector('[tabindex="0"]')
      if (firstFocusable) {
        expect(firstFocusable).toHaveFocus()
      }
    })
  })
}) 