import React from 'react'
import Image from 'next/image'
import { Cog6ToothIcon } from '@heroicons/react/24/outline'

interface HeaderProps {
  logoUrl?: string;
}

export default function Header({ logoUrl }: HeaderProps) {
  return (
    <header className="header bg-white border-b border-gray-200 shadow-sm">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Left side - Logo and Title */}
          <div className="flex items-center space-x-3">
            {logoUrl && (
              <Image
                src={logoUrl}
                alt="Bluesky Post Explorer Logo"
                width={32}
                height={32}
                className="h-8 w-8 rounded"
              />
            )}
            <h1 className="text-xl font-semibold text-gray-900">
              Bluesky Post Explorer
            </h1>
          </div>
          
          {/* Right side - Settings */}
          <button
            type="button"
            disabled
            aria-label="Settings (coming soon)"
            className="p-2 text-gray-400 hover:text-gray-500 disabled:cursor-not-allowed"
            tabIndex={0}
          >
            <Cog6ToothIcon className="h-5 w-5" />
          </button>
        </div>
      </div>
    </header>
  )
} 