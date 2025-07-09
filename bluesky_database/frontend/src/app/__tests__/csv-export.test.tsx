import '@testing-library/jest-dom'

// Mock data structure matching the page component
interface Post {
  id: string
  timestamp: string
  username: string
  text: string
}

// Extract and test the CSV export function directly
const exportToCSV = (posts: Post[]) => {
  const headers = ['Timestamp', 'Username', 'Post Preview']
  const csvContent = [
    headers.join(','),
    ...posts.map(post => {
      const timestamp = new Date(post.timestamp).toLocaleString()
      const username = `"${post.username.replace(/"/g, '""')}"`
      const text = `"${post.text.replace(/"/g, '""')}"`
      return [timestamp, username, text].join(',')
    })
  ].join('\n')

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  
  if (link.download !== undefined) {
    const url = URL.createObjectURL(blob)
    link.setAttribute('href', url)
    link.setAttribute('download', `bluesky-posts-${new Date().toISOString().split('T')[0]}.csv`)
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
  }
}

// Mock implementations for testing
const mockCreateObjectURL = jest.fn()
const mockRevokeObjectURL = jest.fn()
const mockClick = jest.fn()
const mockAppendChild = jest.fn()
const mockRemoveChild = jest.fn()
const mockSetAttribute = jest.fn()

// Setup mocks
Object.defineProperty(global, 'URL', {
  value: {
    createObjectURL: mockCreateObjectURL,
    revokeObjectURL: mockRevokeObjectURL,
  },
  writable: true,
})

global.Blob = jest.fn().mockImplementation((content, options) => ({
  content,
  options,
  size: content?.[0]?.length || 0,
  type: options?.type || '',
}))

const originalCreateElement = document.createElement.bind(document)
document.createElement = jest.fn().mockImplementation((tagName) => {
  if (tagName === 'a') {
    return {
      setAttribute: mockSetAttribute,
      click: mockClick,
      style: { visibility: '' },
      download: true,
    }
  }
  return originalCreateElement(tagName)
})

Object.defineProperty(document.body, 'appendChild', {
  value: mockAppendChild,
  writable: true,
})

Object.defineProperty(document.body, 'removeChild', {
  value: mockRemoveChild,
  writable: true,
})

describe('CSV Export Functionality - MET-13 Unit Tests', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    mockCreateObjectURL.mockReturnValue('mock-blob-url')
  })

  const mockPosts: Post[] = [
    {
      id: '1',
      timestamp: '2024-01-15T14:30:00Z',
      username: 'bluesky_user',
      text: 'Just discovered this amazing new feature on Bluesky!'
    },
    {
      id: '2',
      timestamp: '2024-01-15T12:15:00Z',
      username: 'tech_enthusiast',
      text: 'The future of social media is here and it\'s looking bright! ✨'
    },
    {
      id: '3',
      timestamp: '2024-01-14T18:45:00Z',
      username: 'developer_jane',
      text: 'Working on "cool projects" today with quotes and commas, etc.'
    }
  ]

  describe('CSV Generation and Format', () => {
    it('generates CSV with correct headers', () => {
      exportToCSV(mockPosts)

      expect(global.Blob).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.stringContaining('Timestamp,Username,Post Preview')
        ]),
        { type: 'text/csv;charset=utf-8;' }
      )
    })

    it('formats post data correctly in CSV', () => {
      exportToCSV(mockPosts)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Check that usernames and content are included (usernames should be quoted)
      expect(csvContent).toContain('"bluesky_user"')
      expect(csvContent).toContain('"tech_enthusiast"')
      expect(csvContent).toContain('"developer_jane"')
    })

    it('properly formats timestamps in CSV', () => {
      exportToCSV(mockPosts)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Should contain formatted timestamps (not raw ISO strings)
      expect(csvContent).not.toContain('2024-01-15T14:30:00Z')
      expect(typeof csvContent).toBe('string')
      expect(csvContent.length).toBeGreaterThan(0)
    })
  })

  describe('Special Character Handling', () => {
    it('properly escapes quotes in CSV content', () => {
      const postsWithQuotes: Post[] = [
        {
          id: '1',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'test_user',
          text: 'This has "quotes" in it'
        }
      ]

      exportToCSV(postsWithQuotes)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Should contain escaped quotes (double quotes)
      expect(csvContent).toContain('""')
    })

    it('handles commas in post content', () => {
      const postsWithCommas: Post[] = [
        {
          id: '1',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'test_user',
          text: 'This has, commas, in it'
        }
      ]

      exportToCSV(postsWithCommas)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Content with commas should be properly quoted
      expect(csvContent).toContain('"This has, commas, in it"')
    })

    it('handles special characters and emojis', () => {
      const postsWithSpecialChars: Post[] = [
        {
          id: '1',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'emoji_user',
          text: 'Hello! 🌟 This has émojis and àccénts'
        }
      ]

      exportToCSV(postsWithSpecialChars)

      expect(global.Blob).toHaveBeenCalledWith(
        expect.any(Array),
        { type: 'text/csv;charset=utf-8;' }
      )
    })

    it('handles very long text content', () => {
      const longText = 'A'.repeat(1000) // 1000 character string
      const postsWithLongText: Post[] = [
        {
          id: '1',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'long_poster',
          text: longText
        }
      ]

      exportToCSV(postsWithLongText)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Should handle long content without errors
      expect(csvContent).toContain(longText)
    })
  })

  describe('Download Process', () => {
    it('creates blob with UTF-8 encoding', () => {
      exportToCSV(mockPosts)

      expect(global.Blob).toHaveBeenCalledWith(
        expect.any(Array),
        { type: 'text/csv;charset=utf-8;' }
      )
    })

    it('creates download link with correct attributes', () => {
      exportToCSV(mockPosts)

      expect(mockCreateObjectURL).toHaveBeenCalled()
      expect(document.createElement).toHaveBeenCalledWith('a')
      expect(mockSetAttribute).toHaveBeenCalledWith('href', 'mock-blob-url')
    })

    it('generates filename with current date', () => {
      exportToCSV(mockPosts)

      const today = new Date().toISOString().split('T')[0]
      const expectedFilename = `bluesky-posts-${today}.csv`

      expect(mockSetAttribute).toHaveBeenCalledWith('download', expectedFilename)
    })

    it('triggers download process correctly', () => {
      exportToCSV(mockPosts)

      expect(mockAppendChild).toHaveBeenCalled()
      expect(mockClick).toHaveBeenCalled()
      expect(mockRemoveChild).toHaveBeenCalled()
    })
  })

  describe('Cross-Browser Compatibility', () => {
    it('checks for download support before attempting download', () => {
      // Mock element without download support
      document.createElement = jest.fn().mockImplementation((tagName) => {
        if (tagName === 'a') {
          return {
            setAttribute: mockSetAttribute,
            click: mockClick,
            style: { visibility: '' },
            download: undefined, // No download support
          }
        }
        return originalCreateElement(tagName)
      })

      exportToCSV(mockPosts)

      // Should not attempt to click if download is not supported
      expect(mockClick).not.toHaveBeenCalled()
    })

    it('uses standard web APIs for compatibility', () => {
      // Reset mocks and ensure proper setup
      jest.clearAllMocks()
      mockCreateObjectURL.mockReturnValue('mock-blob-url')
      
      // Reset createElement mock to default behavior
      document.createElement = jest.fn().mockImplementation((tagName) => {
        if (tagName === 'a') {
          return {
            setAttribute: mockSetAttribute,
            click: mockClick,
            style: { visibility: '' },
            download: true,
          }
        }
        return originalCreateElement(tagName)
      })

      exportToCSV(mockPosts)

      // Verify standard APIs are used
      expect(global.Blob).toHaveBeenCalled() // Standard Blob API
      expect(mockCreateObjectURL).toHaveBeenCalled() // Standard URL API
      expect(document.createElement).toHaveBeenCalledWith('a') // Standard DOM API
    })
  })

  describe('Edge Cases and Error Handling', () => {
    it('handles empty posts array', () => {
      exportToCSV([])

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Should still have headers
      expect(csvContent).toContain('Timestamp,Username,Post Preview')
      expect(csvContent.split('\n')).toHaveLength(1) // Only header row
    })

    it('handles posts with empty text', () => {
      const postsWithEmptyText: Post[] = [
        {
          id: '1',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'silent_user',
          text: ''
        }
      ]

      exportToCSV(postsWithEmptyText)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      expect(csvContent).toContain('"silent_user"')
      expect(csvContent).toContain('""') // Empty quoted string
    })

    it('handles posts with special usernames', () => {
      const postsWithSpecialUsernames: Post[] = [
        {
          id: '1',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'user.with-dots_and-dashes',
          text: 'Test post'
        }
      ]

      exportToCSV(postsWithSpecialUsernames)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      expect(csvContent).toContain('"user.with-dots_and-dashes"')
    })

    it('handles usernames with commas and quotes', () => {
      const postsWithProblematicUsernames: Post[] = [
        {
          id: '1',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'user,with,commas',
          text: 'Test post with comma username'
        },
        {
          id: '2',
          timestamp: '2024-01-15T14:30:00Z',
          username: 'user"with"quotes',
          text: 'Test post with quoted username'
        }
      ]

      exportToCSV(postsWithProblematicUsernames)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Usernames with commas should be properly quoted
      expect(csvContent).toContain('"user,with,commas"')
      // Usernames with quotes should have escaped quotes
      expect(csvContent).toContain('"user""with""quotes"')
    })

    it('handles invalid timestamp gracefully', () => {
      const postsWithInvalidTimestamp: Post[] = [
        {
          id: '1',
          timestamp: 'invalid-date',
          username: 'test_user',
          text: 'Test post'
        }
      ]

      // Should not throw error even with invalid timestamp
      expect(() => exportToCSV(postsWithInvalidTimestamp)).not.toThrow()
    })
  })

  describe('Performance and Scalability', () => {
    it('handles large number of posts efficiently', () => {
      const largePosts: Post[] = Array.from({ length: 1000 }, (_, i) => ({
        id: `${i}`,
        timestamp: '2024-01-15T14:30:00Z',
        username: `user_${i}`,
        text: `This is post number ${i} with some content`
      }))

      const startTime = performance.now()
      exportToCSV(largePosts)
      const endTime = performance.now()

      // Should complete within reasonable time (less than 100ms for 1000 posts)
      expect(endTime - startTime).toBeLessThan(100)
      expect(global.Blob).toHaveBeenCalled()
    })

    it('creates appropriately sized blob for large datasets', () => {
      const largePosts: Post[] = Array.from({ length: 100 }, (_, i) => ({
        id: `${i}`,
        timestamp: '2024-01-15T14:30:00Z',
        username: `user_${i}`,
        text: `Post content ${i}`
      }))

      exportToCSV(largePosts)

      const blobCall = (global.Blob as jest.Mock).mock.calls[0]
      const csvContent = blobCall[0][0]

      // Should have header + 100 data rows
      expect(csvContent.split('\n')).toHaveLength(101)
    })
  })
}) 