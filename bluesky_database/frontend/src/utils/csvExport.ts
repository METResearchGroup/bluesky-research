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

export type { Post }