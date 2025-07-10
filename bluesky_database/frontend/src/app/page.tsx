'use client'

import React, { useState } from 'react'
import SearchForm from '@/components/SearchForm'
import ResultsTable from '@/components/ResultsTable'
import ComingSoonPanel from '@/components/ComingSoonPanel'
import { exportToCSV, type Post } from '@/utils/csvExport'

interface SearchFormData {
  query: string
  username: string
  startDate: string
  endDate: string
  exactMatch: boolean
}

// Mock data generator
const generateMockPosts = (searchData: SearchFormData): Post[] => {
  const mockPosts: Post[] = [
    {
      id: '1',
      timestamp: '2024-01-15T14:30:00Z',
      username: 'bluesky_user',
      text: `Just discovered this amazing new feature on Bluesky! The decentralized architecture really shows its potential when you see how fast and reliable the platform is. #bluesky #decentralized`
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
      text: `Working on some cool projects today. Love how the AT Protocol allows for so much innovation in the social media space. Can't wait to see what developers build next! 🚀`
    },
    {
      id: '4',
      timestamp: '2024-01-14T16:22:00Z',
      username: 'social_analyst',
      text: 'Interesting trends emerging in decentralized social networks. The user engagement patterns are quite different from traditional platforms.'
    },
    {
      id: '5',
      timestamp: '2024-01-13T20:10:00Z',
      username: 'creative_mind',
      text: 'Just shared my latest artwork! The creative community on Bluesky is incredibly supportive and diverse. 🎨 #art #creativity'
    },
    {
      id: '6',
      timestamp: '2024-01-13T15:30:00Z',
      username: 'news_curator',
      text: 'Breaking: New developments in social media regulation could impact the entire industry. Decentralized platforms may have advantages in this landscape.'
    },
    {
      id: '7',
      timestamp: '2024-01-12T11:45:00Z',
      username: 'community_builder',
      text: 'Building communities on decentralized platforms feels different - more authentic, more user-controlled. What do you think?'
    },
    {
      id: '8',
      timestamp: '2024-01-12T09:20:00Z',
      username: 'policy_wonk',
      text: 'The implications of decentralized social media for digital rights and privacy are fascinating. We\'re witnessing a paradigm shift.'
    }
  ]

  // Simple filtering based on search criteria
  let filteredPosts = mockPosts

  if (searchData.query) {
    const query = searchData.query.toLowerCase()
    filteredPosts = filteredPosts.filter(post => {
      if (searchData.exactMatch) {
        return post.text.toLowerCase().includes(query) || post.username.toLowerCase().includes(query)
      } else {
        return post.text.toLowerCase().includes(query) || post.username.toLowerCase().includes(query)
      }
    })
  }

  if (searchData.username) {
    filteredPosts = filteredPosts.filter(post => 
      post.username.toLowerCase().includes(searchData.username.toLowerCase())
    )
  }

  if (searchData.startDate) {
    filteredPosts = filteredPosts.filter(post => 
      new Date(post.timestamp) >= new Date(searchData.startDate)
    )
  }

  if (searchData.endDate) {
    filteredPosts = filteredPosts.filter(post => 
      new Date(post.timestamp) <= new Date(searchData.endDate)
    )
  }

  return filteredPosts
}



export default function HomePage() {
  const [posts, setPosts] = useState<Post[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [hasSearched, setHasSearched] = useState(false)

  const handleSearch = async (searchData: SearchFormData) => {
    setIsLoading(true)
    setHasSearched(true)
    
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 1500))
    
    const results = generateMockPosts(searchData)
    setPosts(results)
    setIsLoading(false)
  }

  const handleExportCSV = () => {
    exportToCSV(posts)
  }

  return (
    <div className="space-y-8">
      {/* Hero Section */}
      <div className="text-center">
        <h1 className="text-3xl font-bold text-gray-900 sm:text-4xl">
          Explore Bluesky Posts
        </h1>
        <p className="mt-3 max-w-md mx-auto text-base text-gray-500 sm:text-lg md:mt-5 md:text-xl md:max-w-3xl">
          Search and analyze Bluesky posts with advanced filtering options. 
          Export results for further analysis and research.
        </p>
      </div>

      {/* Search Form */}
      <SearchForm onSubmit={handleSearch} isLoading={isLoading} />

      {/* Coming Soon Panel */}
      <ComingSoonPanel />

      {/* Results Section */}
      {(hasSearched || posts.length > 0) && (
        <ResultsTable 
          posts={posts} 
          isLoading={isLoading}
          onExportCSV={posts.length > 0 ? handleExportCSV : undefined}
        />
      )}

      {/* Footer */}
      <footer className="mt-16 border-t border-gray-200 pt-8">
        <div className="text-center">
          <p className="text-sm text-gray-500">
            Built with Next.js, TypeScript, and Tailwind CSS. 
            <span className="block mt-2">
              <a 
                href="https://github.com" 
                className="text-blue-600 hover:text-blue-800"
                target="_blank"
                rel="noopener noreferrer"
              >
                View on GitHub
              </a>
              {' • '}
              <a 
                href="mailto:contact@example.com" 
                className="text-blue-600 hover:text-blue-800"
              >
                Contact
              </a>
              {' • '}
              <a 
                href="/api/docs" 
                className="text-blue-600 hover:text-blue-800"
              >
                API Documentation
              </a>
            </span>
          </p>
        </div>
      </footer>
    </div>
  )
} 