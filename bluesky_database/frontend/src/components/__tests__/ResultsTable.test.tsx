import React from 'react'
import { render, screen } from '@testing-library/react'
import { userEvent } from '@testing-library/user-event'
import ResultsTable from '../ResultsTable'

// Mock data for testing
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
  },
  {
    id: '3',
    timestamp: '2025-01-20T12:00:00Z', 
    username: 'user3',
    text: 'Another post with normal length content.'
  }
]

const longTextPost = {
  id: '4',
  timestamp: '2025-01-20T13:00:00Z',
  username: 'longuser',
  text: 'This is an extremely long post that definitely exceeds the 140 character truncation limit. It contains multiple sentences and should demonstrate the text expansion functionality properly. When expanded, users should be able to see the full content and then collapse it back using the show less button.'
}

describe('ResultsTable Component', () => {
  const user = userEvent.setup()

  describe('Loading States', () => {
    test('displays loading skeleton when isLoading is true', () => {
      render(<ResultsTable posts={[]} isLoading={true} />)
      
      expect(screen.getByText('Search Results')).toBeInTheDocument()
      
      // Check for loading skeleton elements
      const skeletonElements = document.querySelectorAll('.animate-pulse')
      expect(skeletonElements.length).toBeGreaterThan(0)
    })

    test('displays loading skeleton with proper accessibility', () => {
      render(<ResultsTable posts={[]} isLoading={true} />)
      
      // Verify semantic structure during loading
      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveTextContent('Search Results')
    })
  })

  describe('Empty States', () => {
    test('displays empty state when no posts and not loading', () => {
      render(<ResultsTable posts={[]} isLoading={false} />)
      
      expect(screen.getByText('No results found')).toBeInTheDocument()
      expect(screen.getByText('Try adjusting your search criteria or search terms.')).toBeInTheDocument()
      
      // Check for search icon in empty state
      const searchIcon = document.querySelector('svg')
      expect(searchIcon).toBeInTheDocument()
    })

    test('does not show export button when no posts', () => {
      const mockExport = jest.fn()
      render(<ResultsTable posts={[]} isLoading={false} onExportCSV={mockExport} />)
      
      expect(screen.queryByText('Export CSV')).not.toBeInTheDocument()
    })
  })

  describe('Data Display', () => {
    test('displays correct number of results in heading', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      expect(screen.getByText('Search Results (3)')).toBeInTheDocument()
    })

    test('displays all required columns in desktop table view', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      // Check table headers
      expect(screen.getByText('Timestamp')).toBeInTheDocument()
      expect(screen.getByText('Username')).toBeInTheDocument() 
      expect(screen.getByText('Post Preview')).toBeInTheDocument()
    })

    test('displays post data correctly in table rows', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      // Check that usernames are displayed with @ prefix (appears in both desktop and mobile)
      expect(screen.getAllByText('@user1').length).toBe(2)
      expect(screen.getAllByText('@user2').length).toBe(2)
      expect(screen.getAllByText('@user3').length).toBe(2)
      
      // Check that post content is displayed (appears in both desktop and mobile)
      expect(screen.getAllByText('This is a short post that should not be truncated.').length).toBe(2)
      expect(screen.getAllByText('Another post with normal length content.').length).toBe(2)
    })

    test('formats timestamps correctly', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      // Should format timestamp to readable format
      // The exact format depends on locale, but it should be readable
      const timestampElements = screen.getAllByText(/Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec/)
      expect(timestampElements.length).toBeGreaterThan(0)
    })

    test('handles invalid timestamps gracefully', () => {
      const invalidTimestampPost = [{
        id: '1',
        timestamp: 'invalid-date',
        username: 'user1', 
        text: 'Test post'
      }]
      
      render(<ResultsTable posts={invalidTimestampPost} isLoading={false} />)
      
      // Should display "Invalid Date" when parsing fails
      const invalidDates = screen.getAllByText('Invalid Date')
      expect(invalidDates.length).toBe(2) // Desktop + mobile views
    })
  })

  describe('Text Truncation', () => {
    test('truncates long text and shows "Show more" button', () => {
      render(<ResultsTable posts={[longTextPost]} isLoading={false} />)
      
      // Should show truncated text ending with "..."
      const truncatedTexts = screen.getAllByText(/This is an extremely long post.*\.\.\./)
      expect(truncatedTexts.length).toBeGreaterThan(0)
      
      // Should show "Show more" button (appears in both desktop and mobile views)
      const showMoreButtons = screen.getAllByText('Show more')
      expect(showMoreButtons.length).toBe(2) // desktop + mobile
    })

    test('expands text when "Show more" is clicked', async () => {
      render(<ResultsTable posts={[longTextPost]} isLoading={false} />)
      
      const showMoreButtons = screen.getAllByText('Show more')
      await user.click(showMoreButtons[0]) // Click first one (desktop)
      
      // Should show full text
      const fullTexts = screen.getAllByText(longTextPost.text)
      expect(fullTexts.length).toBeGreaterThan(0)
      
      // Should show "Show less" button
      const showLessButtons = screen.getAllByText('Show less')
      expect(showLessButtons.length).toBeGreaterThan(0)
    })

    test('collapses text when "Show less" is clicked', async () => {
      render(<ResultsTable posts={[longTextPost]} isLoading={false} />)
      
      // First expand
      const showMoreButtons = screen.getAllByText('Show more')
      await user.click(showMoreButtons[0])
      
      // Then collapse
      const showLessButtons = screen.getAllByText('Show less')
      await user.click(showLessButtons[0])
      
      // Should show truncated text again
      const truncatedTexts = screen.getAllByText(/This is an extremely long post.*\.\.\./)
      expect(truncatedTexts.length).toBeGreaterThan(0)
      
      // Should show "Show more" button again
      const newShowMoreButtons = screen.getAllByText('Show more')
      expect(newShowMoreButtons.length).toBe(2)
    })

    test('does not show expand buttons for short text', () => {
      const shortPost = {
        id: '1',
        timestamp: '2025-01-20T10:30:00Z',
        username: 'user1',
        text: 'Short text'
      }
      
      render(<ResultsTable posts={[shortPost]} isLoading={false} />)
      
      expect(screen.queryByText('Show more')).not.toBeInTheDocument()
      expect(screen.queryByText('Show less')).not.toBeInTheDocument()
    })
  })

  describe('CSV Export', () => {
    test('displays export button when posts exist and onExportCSV provided', () => {
      const mockExport = jest.fn()
      render(<ResultsTable posts={mockPosts} isLoading={false} onExportCSV={mockExport} />)
      
      const exportButton = screen.getByText('Export CSV')
      expect(exportButton).toBeInTheDocument()
      expect(exportButton).toHaveClass('bg-green-600')
    })

    test('calls onExportCSV when export button is clicked', async () => {
      const mockExport = jest.fn()
      render(<ResultsTable posts={mockPosts} isLoading={false} onExportCSV={mockExport} />)
      
      const exportButton = screen.getByText('Export CSV')
      await user.click(exportButton)
      
      expect(mockExport).toHaveBeenCalledTimes(1)
    })

    test('does not display export button when onExportCSV not provided', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      expect(screen.queryByText('Export CSV')).not.toBeInTheDocument()
    })
  })

  describe('Responsive Design', () => {
    test('shows table on desktop and cards on mobile', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      // Desktop table should be hidden on mobile (class: hidden md:block)
      const table = document.querySelector('table')
      expect(table?.parentElement).toHaveClass('hidden', 'md:block')
      
      // Mobile cards should be hidden on desktop (class: md:hidden)
      const mobileContainer = document.querySelector('.md\\:hidden')
      expect(mobileContainer).toBeInTheDocument()
    })

    test('displays correct structure in mobile cards', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      // Mobile cards should contain username and timestamp in specific layout
      const mobileCards = document.querySelectorAll('.md\\:hidden .bg-gray-50')
      expect(mobileCards.length).toBe(mockPosts.length)
    })
  })

  describe('Accessibility', () => {
    test('has proper heading structure', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveTextContent('Search Results (3)')
    })

    test('table has proper semantic structure', () => {
      render(<ResultsTable posts={mockPosts} isLoading={false} />)
      
      const table = screen.getByRole('table')
      expect(table).toBeInTheDocument()
      
      const columnHeaders = screen.getAllByRole('columnheader')
      expect(columnHeaders).toHaveLength(3)
      
      const rows = screen.getAllByRole('row')
      // Headers + data rows
      expect(rows.length).toBe(mockPosts.length + 1)
    })

    test('buttons have proper accessibility and keyboard interaction', async () => {
      render(<ResultsTable posts={[longTextPost]} isLoading={false} />)
      
      const showMoreButtons = screen.getAllByText('Show more')
      
      // Test keyboard interaction
      showMoreButtons[0].focus()
      expect(showMoreButtons[0]).toHaveFocus()
      
      // Test that buttons are clickable
      expect(showMoreButtons[0]).toBeEnabled()
      expect(showMoreButtons[0].tagName).toBe('BUTTON')
    })

    test('export button has proper accessibility', () => {
      const mockExport = jest.fn()
      render(<ResultsTable posts={mockPosts} isLoading={false} onExportCSV={mockExport} />)
      
      const exportButton = screen.getByRole('button', { name: /export csv/i })
      expect(exportButton).toBeInTheDocument()
    })
  })

  describe('Performance', () => {
    test('handles large number of posts efficiently', () => {
      const largePosts = Array.from({ length: 100 }, (_, i) => ({
        id: `post-${i}`,
        timestamp: '2025-01-20T10:30:00Z',
        username: `user${i}`,
        text: `Post content ${i}`
      }))
      
      const startTime = performance.now()
      render(<ResultsTable posts={largePosts} isLoading={false} />)
      const endTime = performance.now()
      
      // Rendering should complete within reasonable time (500ms)
      expect(endTime - startTime).toBeLessThan(500)
      
      // All posts should be rendered
      expect(screen.getByText('Search Results (100)')).toBeInTheDocument()
    })

    test('maintains expansion state correctly with multiple posts', async () => {
      const multipleLongPosts = [longTextPost, { ...longTextPost, id: '5', username: 'user5' }]
      render(<ResultsTable posts={multipleLongPosts} isLoading={false} />)
      
      const showMoreButtons = screen.getAllByText('Show more')
      expect(showMoreButtons).toHaveLength(4) // 2 posts × 2 views (desktop + mobile)
      
      // Expand first post (desktop view)
      await user.click(showMoreButtons[0])
      
      // Should have both "Show less" and remaining "Show more" buttons
      const showLessButtons = screen.getAllByText('Show less')
      const remainingShowMoreButtons = screen.getAllByText('Show more')
      expect(showLessButtons.length).toBeGreaterThan(0)
      expect(remainingShowMoreButtons.length).toBeGreaterThan(0)
    })
  })

  describe('Edge Cases', () => {
    test('handles empty strings gracefully', () => {
      const emptyPost = {
        id: '1',
        timestamp: '',
        username: '',
        text: ''
      }
      
      render(<ResultsTable posts={[emptyPost]} isLoading={false} />)
      
      expect(screen.getByText('Search Results (1)')).toBeInTheDocument()
      const atSymbols = screen.getAllByText('@')
      expect(atSymbols.length).toBe(2) // Desktop + mobile views
    })

    test('handles special characters in text', () => {
      const specialCharPost = {
        id: '1',
        timestamp: '2025-01-20T10:30:00Z',
        username: 'user1',
        text: 'Post with special chars: @mention #hashtag https://example.com 🚀 & < > " \''
      }
      
      render(<ResultsTable posts={[specialCharPost]} isLoading={false} />)
      
      const specialTexts = screen.getAllByText(/Post with special chars/)
      expect(specialTexts.length).toBe(2) // Desktop + mobile views
    })

    test('handles very long usernames', () => {
      const longUsernamePost = {
        id: '1', 
        timestamp: '2025-01-20T10:30:00Z',
        username: 'verylongusernamethatexceedsnormallengthexpectations123456789',
        text: 'Test post'
      }
      
      render(<ResultsTable posts={[longUsernamePost]} isLoading={false} />)
      
      const longUsernames = screen.getAllByText('@verylongusernamethatexceedsnormallengthexpectations123456789')
      expect(longUsernames.length).toBe(2) // Desktop + mobile views
    })
  })

  describe('Component Props', () => {
    test('works with default props', () => {
      render(<ResultsTable posts={[]} />)
      
      expect(screen.getByText('No results found')).toBeInTheDocument()
    })

    test('handles missing optional props gracefully', () => {
      render(<ResultsTable posts={mockPosts} />)
      
      expect(screen.getByText('Search Results (3)')).toBeInTheDocument()
      expect(screen.queryByText('Export CSV')).not.toBeInTheDocument()
    })
  })
}) 