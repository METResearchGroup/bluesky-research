interface Post {
  id: string
  timestamp: string
  username: string
  text: string
}

/**
 * Helper function to escape and quote CSV fields according to RFC 4180
 * @param field - The field value to escape
 * @returns The properly escaped and quoted field
 */
export const escapeCSVField = (field: string | null | undefined): string => {
  // Handle null/undefined values
  if (field == null) {
    return '""'
  }
  
  // Always quote fields to ensure RFC 4180 compliance
  // Escape any existing quotes by doubling them
  const escapedField = field.replace(/"/g, '""')
  return `"${escapedField}"`
}

/**
 * Exports an array of posts to a CSV file and triggers download
 * @param posts - Array of Post objects to export
 */
export const exportToCSV = (posts: Post[]): void => {
  const headers = ['Timestamp', 'Username', 'Post Preview']
  const csvContent = [
    headers.map(escapeCSVField).join(','),
    ...posts.map(post => {
      const date = new Date(post.timestamp)
      const timestamp = isNaN(date.getTime()) ? post.timestamp : date.toLocaleString()
      const username = post.username
      const text = post.text
      return [
        escapeCSVField(timestamp),
        escapeCSVField(username),
        escapeCSVField(text)
      ].join(',')
    })
  ].join('\n')

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  const url = URL.createObjectURL(blob)
  
  if (link.download !== undefined) {
    // Modern browsers with download attribute support
    link.setAttribute('href', url)
    link.setAttribute('download', `bluesky-posts-${new Date().toISOString().split('T')[0]}.csv`)
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    
    // Clean up the blob URL after a short delay to ensure download completes
    setTimeout(() => {
      URL.revokeObjectURL(url)
    }, 100)
  } else {
    // Fallback for older browsers without download attribute support
    try {
      window.open(url, '_blank')
      
      // Delay cleanup to prevent race condition - give browser time to process the request
      setTimeout(() => {
        URL.revokeObjectURL(url)
      }, 1000)
    } catch (error) {
      // Final fallback: navigate to the blob URL in the same window
      window.location.href = url

      console.log('error', error)
      
      // Clean up after a longer delay since navigation might take more time
      setTimeout(() => {
        URL.revokeObjectURL(url)
      }, 3000)
    }
  }
}

export type { Post }