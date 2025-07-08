'use client'

import React, { useState } from 'react'

interface Post {
  id: string
  timestamp: string
  username: string
  text: string
}

interface ResultsTableProps {
  posts: Post[]
  isLoading?: boolean
  onExportCSV?: () => void
}

export default function ResultsTable({ posts = [], isLoading = false, onExportCSV }: ResultsTableProps) {
  const [expandedPosts, setExpandedPosts] = useState<Set<string>>(new Set())

  const toggleExpanded = (postId: string) => {
    const newExpanded = new Set(expandedPosts)
    if (newExpanded.has(postId)) {
      newExpanded.delete(postId)
    } else {
      newExpanded.add(postId)
    }
    setExpandedPosts(newExpanded)
  }

  const formatTimestamp = (timestamp: string) => {
    try {
      return new Date(timestamp).toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
    } catch {
      return timestamp
    }
  }

  const truncateText = (text: string, maxLength: number = 140) => {
    if (text.length <= maxLength) return text
    return text.substring(0, maxLength) + '...'
  }

  // Loading skeleton component
  const LoadingSkeleton = () => (
    <div className="space-y-4">
      {[...Array(10)].map((_, i) => (
        <div key={i} className="bg-white p-4 rounded-lg border animate-pulse">
          <div className="flex flex-col md:flex-row md:items-center space-y-2 md:space-y-0 md:space-x-4">
            <div className="h-4 bg-gray-300 rounded w-32"></div>
            <div className="h-4 bg-gray-300 rounded w-24"></div>
            <div className="h-4 bg-gray-300 rounded flex-1"></div>
          </div>
        </div>
      ))}
    </div>
  )

  // Empty state component
  const EmptyState = () => (
    <div className="text-center py-12">
      <div className="mx-auto h-12 w-12 text-gray-400">
        <svg fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
        </svg>
      </div>
      <h3 className="mt-2 text-sm font-medium text-gray-900">No results found</h3>
      <p className="mt-1 text-sm text-gray-500">
        Try adjusting your search criteria or search terms.
      </p>
    </div>
  )

  if (isLoading) {
    return (
      <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-medium text-gray-900">Search Results</h2>
          <div className="h-4 bg-gray-300 rounded w-20 animate-pulse"></div>
        </div>
        <LoadingSkeleton />
      </div>
    )
  }

  return (
    <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-6">
        <h2 className="text-lg font-medium text-gray-900 mb-2 sm:mb-0">
          Search Results {posts.length > 0 && `(${posts.length})`}
        </h2>
        {posts.length > 0 && onExportCSV && (
          <button
            onClick={onExportCSV}
            className="inline-flex items-center px-4 py-2 bg-green-600 text-white text-sm font-medium rounded-md hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2"
          >
            <svg className="h-4 w-4 mr-2" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
            </svg>
            Export CSV
          </button>
        )}
      </div>

      {posts.length === 0 ? (
        <EmptyState />
      ) : (
        <>
          {/* Desktop Table View */}
          <div className="hidden md:block overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Timestamp
                  </th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Username
                  </th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Post Preview
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {posts.map((post) => (
                  <tr key={post.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {formatTimestamp(post.timestamp)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      @{post.username}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900">
                      <div className="max-w-md">
                        {expandedPosts.has(post.id) ? (
                          <div>
                            <p className="whitespace-pre-wrap">{post.text}</p>
                            {post.text.length > 140 && (
                              <button
                                onClick={() => toggleExpanded(post.id)}
                                className="mt-1 text-blue-600 hover:text-blue-800 text-sm"
                              >
                                Show less
                              </button>
                            )}
                          </div>
                        ) : (
                          <div>
                            <p>{truncateText(post.text)}</p>
                            {post.text.length > 140 && (
                              <button
                                onClick={() => toggleExpanded(post.id)}
                                className="mt-1 text-blue-600 hover:text-blue-800 text-sm"
                              >
                                Show more
                              </button>
                            )}
                          </div>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Mobile Card View */}
          <div className="md:hidden space-y-4">
            {posts.map((post) => (
              <div key={post.id} className="bg-gray-50 rounded-lg p-4 border">
                <div className="flex flex-col space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium text-gray-900">@{post.username}</span>
                    <span className="text-xs text-gray-500">{formatTimestamp(post.timestamp)}</span>
                  </div>
                  <div>
                    {expandedPosts.has(post.id) ? (
                      <div>
                        <p className="text-sm text-gray-700 whitespace-pre-wrap">{post.text}</p>
                        {post.text.length > 140 && (
                          <button
                            onClick={() => toggleExpanded(post.id)}
                            className="mt-1 text-blue-600 hover:text-blue-800 text-sm"
                          >
                            Show less
                          </button>
                        )}
                      </div>
                    ) : (
                      <div>
                        <p className="text-sm text-gray-700">{truncateText(post.text)}</p>
                        {post.text.length > 140 && (
                          <button
                            onClick={() => toggleExpanded(post.id)}
                            className="mt-1 text-blue-600 hover:text-blue-800 text-sm"
                          >
                            Show more
                          </button>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
} 