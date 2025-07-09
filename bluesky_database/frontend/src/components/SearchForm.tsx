'use client'

import React from 'react'
import { useForm } from 'react-hook-form'
import { MagnifyingGlassIcon } from '@heroicons/react/24/outline'

interface SearchFormData {
  query: string
  username: string
  startDate: string
  endDate: string
  exactMatch: boolean
}

interface SearchFormProps {
  onSubmit: (data: SearchFormData) => Promise<void>
  isLoading?: boolean
}



export default function SearchForm({ onSubmit, isLoading = false }: SearchFormProps) {
  const {
    register,
    handleSubmit,
    formState: { errors },
    watch,
  } = useForm<SearchFormData>({
    defaultValues: {
      query: '',
      username: '',
      startDate: '',
      endDate: '',
      exactMatch: false,
    }
  })

  const startDate = watch('startDate')

  // Validate that end date is after start date
  const validateDateRange = (endDate: string) => {
    if (!startDate || !endDate) return true
    return new Date(endDate) >= new Date(startDate) || 'End date must be after start date'
  }

  // Validate username format (optional field)
  const validateUsername = (username: string) => {
    if (!username) return true
    const usernameRegex = /^[a-zA-Z0-9._-]+$/
    return usernameRegex.test(username) || 'Username can only contain letters, numbers, dots, hyphens, and underscores'
  }

  const handleFormSubmit = async (data: SearchFormData) => {
    try {
      await onSubmit(data)
    } catch (error) {
      console.error('Search failed:', error)
    }
  }

  return (
    <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
      <h2 className="text-lg font-medium text-gray-900 mb-6">Search Posts</h2>
      
      <form onSubmit={handleSubmit(handleFormSubmit)} className="space-y-4" role="form">
        {/* Search Query */}
        <div>
          <label htmlFor="query" className="block text-sm font-medium text-gray-700 mb-1">
            Search Query <span className="text-red-500">*</span>
          </label>
          <input
            {...register('query', { 
              required: 'Search query is required',
              minLength: { value: 1, message: 'Query must be at least 1 character' }
            })}
            type="text"
            id="query"
            placeholder="Enter text or #hashtag to search..."
            aria-required="true"
            aria-describedby={errors.query ? "query-error" : undefined}
            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
          {errors.query && (
            <p id="query-error" className="mt-1 text-sm text-red-600" role="alert">
              {errors.query.message}
            </p>
          )}
        </div>

        {/* Username Filter */}
        <div>
          <label htmlFor="username" className="block text-sm font-medium text-gray-700 mb-1">
            Username Filter
          </label>
          <input
            {...register('username', { validate: validateUsername })}
            type="text"
            id="username"
            placeholder="Optional: filter by username"
            aria-describedby={errors.username ? "username-error" : undefined}
            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
          {errors.username && (
            <p id="username-error" className="mt-1 text-sm text-red-600" role="alert">
              {errors.username.message}
            </p>
          )}
        </div>

        {/* Date Range */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label htmlFor="startDate" className="block text-sm font-medium text-gray-700 mb-1">
              Start Date
            </label>
            <input
              {...register('startDate')}
              type="date"
              id="startDate"
              data-testid="start-date-picker"
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
          <div>
            <label htmlFor="endDate" className="block text-sm font-medium text-gray-700 mb-1">
              End Date
            </label>
            <input
              {...register('endDate', { validate: validateDateRange })}
              type="date"
              id="endDate"
              data-testid="end-date-picker"
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            {errors.endDate && (
              <p className="mt-1 text-sm text-red-600" role="alert">
                {errors.endDate.message}
              </p>
            )}
          </div>
        </div>

        {/* Exact Match Toggle */}
        <div className="flex items-center">
          <input
            {...register('exactMatch')}
            type="checkbox"
            id="exactMatch"
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="exactMatch" className="ml-2 block text-sm text-gray-700">
            Exact match search
          </label>
        </div>

        {/* Submit Button */}
        <button
          type="submit"
          disabled={isLoading}
          className="w-full md:w-auto flex items-center justify-center px-6 py-2 bg-blue-600 text-white font-medium rounded-md shadow-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {isLoading ? (
            <>
              <div className="animate-spin -ml-1 mr-3 h-4 w-4 border-2 border-white border-t-transparent rounded-full" />
              Searching...
            </>
          ) : (
            <>
              <MagnifyingGlassIcon className="h-4 w-4 mr-2" />
              Search Posts
            </>
          )}
        </button>
      </form>
    </div>
  )
} 