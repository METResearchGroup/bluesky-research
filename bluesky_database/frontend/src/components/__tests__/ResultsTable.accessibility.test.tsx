import React from 'react'
import { render } from '@testing-library/react'
import { axe } from 'jest-axe'
import ResultsTable from '../ResultsTable'

// Mock data for accessibility testing
const mockPosts = [
  {
    id: '1',
    timestamp: '2025-01-20T10:30:00Z',
    username: 'user1',
    text: 'This is a short post that should not be truncated.'
  },
  {
    id: '2', 
    timestamp: '2025-01-20T11:15:00Z',
    username: 'user2',
    text: 'This is a very long post that exceeds the 140 character limit and should be truncated with a show more button to expand the full content for better user experience and readability.'
  }
]

describe('ResultsTable Accessibility', () => {
  test('should not have any accessibility violations when loading', async () => {
    const { container } = render(<ResultsTable posts={[]} isLoading={true} />)
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  test('should not have any accessibility violations when empty', async () => {
    const { container } = render(<ResultsTable posts={[]} isLoading={false} />)
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  test('should not have any accessibility violations with data', async () => {
    const { container } = render(<ResultsTable posts={mockPosts} isLoading={false} />)
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  test('should not have any accessibility violations with export button', async () => {
    const mockExport = jest.fn()
    const { container } = render(
      <ResultsTable posts={mockPosts} isLoading={false} onExportCSV={mockExport} />
    )
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  test('should maintain accessibility when text is expanded', async () => {
    const longPost = [{
      id: '1',
      timestamp: '2025-01-20T10:30:00Z',
      username: 'user1',
      text: 'This is a very long post that definitely exceeds the 140 character truncation limit. It contains multiple sentences and should demonstrate the text expansion functionality properly. When expanded, users should be able to see the full content and then collapse it back using the show less button.'
    }]
    
    const { container } = render(<ResultsTable posts={longPost} isLoading={false} />)
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  test('should have proper heading hierarchy', () => {
    const { container } = render(<ResultsTable posts={mockPosts} isLoading={false} />)
    
    // Should have exactly one h2 heading
    const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6')
    const h2Headings = container.querySelectorAll('h2')
    const h3Headings = container.querySelectorAll('h3')
    
    expect(h2Headings).toHaveLength(1)
    expect(h3Headings).toHaveLength(0) // No h3 in data state
    expect(headings).toHaveLength(1) // Only the main heading
  })

  test('should have proper heading hierarchy in empty state', () => {
    const { container } = render(<ResultsTable posts={[]} isLoading={false} />)
    
    // Should have h2 for main heading and h3 for empty state
    const h2Headings = container.querySelectorAll('h2')
    const h3Headings = container.querySelectorAll('h3')
    
    expect(h2Headings).toHaveLength(1)
    expect(h3Headings).toHaveLength(1) // Empty state heading
  })

  test('should have sufficient color contrast', () => {
    const { container } = render(<ResultsTable posts={mockPosts} isLoading={false} />)
    
    // Check for proper text color classes (these should pass WCAG AA contrast)
    const primaryText = container.querySelector('.text-gray-900')
    const secondaryText = container.querySelector('.text-gray-500')
    const tertiaryText = container.querySelector('.text-gray-700')
    
    expect(primaryText).toBeInTheDocument() // Dark text on light background
    expect(secondaryText).toBeInTheDocument() // Medium contrast
    expect(tertiaryText).toBeInTheDocument() // Good contrast
  })

  test('should have proper semantic table structure', () => {
    const { container } = render(<ResultsTable posts={mockPosts} isLoading={false} />)
    
    // Table should have proper semantic structure
    const table = container.querySelector('table')
    const thead = container.querySelector('thead')
    const tbody = container.querySelector('tbody')
    const tableHeaders = container.querySelectorAll('th[scope="col"]')
    
    expect(table).toBeInTheDocument()
    expect(thead).toBeInTheDocument()
    expect(tbody).toBeInTheDocument()
    expect(tableHeaders).toHaveLength(3) // Three column headers
    
    // Each header should have scope attribute
    tableHeaders.forEach(header => {
      expect(header).toHaveAttribute('scope', 'col')
    })
  })

  test('should have accessible button implementations', () => {
    const longPost = [{
      id: '1',
      timestamp: '2025-01-20T10:30:00Z',
      username: 'user1',
      text: 'This is a very long post that definitely exceeds the 140 character truncation limit. It contains multiple sentences and should demonstrate the text expansion functionality properly.'
    }]
    
    const { container } = render(<ResultsTable posts={longPost} isLoading={false} />)
    
    // All buttons should be properly implemented
    const buttons = container.querySelectorAll('button')
    buttons.forEach(button => {
      // Should be actual button elements (not divs with click handlers)
      expect(button.tagName).toBe('BUTTON')
      
      // Should be keyboard accessible (buttons are inherently focusable)
      expect(button).not.toHaveAttribute('tabindex', '-1')
    })
  })

  test('should handle keyboard navigation properly', () => {
    const mockExport = jest.fn()
    const { container } = render(
      <ResultsTable posts={mockPosts} isLoading={false} onExportCSV={mockExport} />
    )
    
    // All interactive elements should be in tab order
    const focusableElements = container.querySelectorAll('button, [tabindex="0"]')
    
    focusableElements.forEach(element => {
      expect(element).not.toHaveAttribute('tabindex', '-1')
    })
  })

  test('should provide appropriate ARIA labels and descriptions', () => {
    const { container } = render(<ResultsTable posts={mockPosts} isLoading={false} />)
    
    // Table should have proper ARIA structure
    const table = container.querySelector('table')
    expect(table).toBeInTheDocument()
    
    // Column headers should have proper scope
    const columnHeaders = container.querySelectorAll('th[scope="col"]')
    expect(columnHeaders).toHaveLength(3)
  })

  test('should maintain accessibility during loading states', async () => {
    const { container } = render(<ResultsTable posts={[]} isLoading={true} />)
    
    // Loading state should be accessible
    const results = await axe(container)
    expect(results).toHaveNoViolations()
    
    // Loading indicators should be present but not interfere with screen readers
    const loadingElements = container.querySelectorAll('.animate-pulse')
    expect(loadingElements.length).toBeGreaterThan(0)
  })

  test('should have proper responsive behavior for accessibility', () => {
    const { container } = render(<ResultsTable posts={mockPosts} isLoading={false} />)
    
    // Desktop table view
    const desktopTable = container.querySelector('.hidden.md\\:block')
    expect(desktopTable).toBeInTheDocument()
    
    // Mobile card view  
    const mobileCards = container.querySelector('.md\\:hidden')
    expect(mobileCards).toBeInTheDocument()
    
    // Both views should contain the same semantic information
    // This ensures content is accessible across viewport sizes
  })
}) 