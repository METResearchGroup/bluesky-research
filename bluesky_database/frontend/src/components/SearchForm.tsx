'use client'

import React from 'react'
import { useForm, Controller } from 'react-hook-form'
import { MagnifyingGlassIcon, CalendarIcon } from '@heroicons/react/24/outline'
import { Popover, PopoverButton, PopoverPanel } from '@headlessui/react'

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

interface DatePickerProps {
  label: string
  value: string
  onChange: (date: string) => void
  placeholder: string
  id: string
  testId: string
  error?: string
}

function DatePicker({ label, value, onChange, placeholder, id, testId, error }: DatePickerProps) {
  const labelId = `${id}-label`
  const buttonId = `${id}-button`

  const handleDateChange = (event: React.ChangeEvent<HTMLInputElement>, close: () => void) => {
    onChange(event.target.value)
    close()
  }

  return (
    <div>
      <label id={labelId} htmlFor={buttonId} className="block text-sm font-medium text-gray-700 mb-1">
        {label}
      </label>
      <Popover className="relative">
        {({ open, close }) => (
          <>
            <PopoverButton
              id={buttonId}
              data-testid={testId}
              role="button"
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 flex items-center justify-between bg-white text-left"
              aria-haspopup="dialog"
              aria-expanded={open}
              aria-labelledby={labelId}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault()
                } else if (event.key === 'Escape' && open) {
                  event.preventDefault()
                  close()
                }
              }}
            >
              <span className={value ? 'text-gray-900' : 'text-gray-400'}>
                {value || placeholder}
              </span>
              <CalendarIcon className="h-4 w-4 text-gray-400" />
            </PopoverButton>
            
            <PopoverPanel 
              data-testid="date-picker-dialog"
              className="absolute z-10 mt-1 w-full bg-white border border-gray-300 rounded-md shadow-lg p-2"
              onKeyDown={(event) => {
                if (event.key === 'Escape') {
                  event.preventDefault()
                  close()
                }
              }}
            >
              <input
                type="date"
                value={value}
                onChange={(event) => handleDateChange(event, close)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                autoFocus
                tabIndex={0}
              />
            </PopoverPanel>
          </>
        )}
      </Popover>
      {error && (
        <p className="mt-1 text-sm text-red-600" role="alert">
          {error}
        </p>
      )}
    </div>
  )
}

export default function SearchForm({ onSubmit, isLoading = false }: SearchFormProps) {
  const {
    register,
    handleSubmit,
    formState: { errors },
    watch,
    control,
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

        {/* Date Range using Headless UI DatePickers */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Controller
            name="startDate"
            control={control}
            render={({ field }) => (
              <DatePicker
                label="Start Date"
                value={field.value}
                onChange={field.onChange}
                placeholder="Select start date"
                id="startDate"
                testId="start-date-picker"
              />
            )}
          />
          <Controller
            name="endDate"
            control={control}
            rules={{ validate: validateDateRange }}
            render={({ field }) => (
              <DatePicker
                label="End Date"
                value={field.value}
                onChange={field.onChange}
                placeholder="Select end date"
                id="endDate"
                testId="end-date-picker"
                error={errors.endDate?.message}
              />
            )}
          />
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