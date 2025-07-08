'use client'

import React, { useState } from 'react'

interface ComingSoonFeature {
  id: string
  label: string
  description: string
}

const comingSoonFeatures: ComingSoonFeature[] = [
  {
    id: 'political',
    label: 'Political Content Filter',
    description: 'Filter posts by political content and sentiment analysis'
  },
  {
    id: 'outrage',
    label: 'Outrage Detection',
    description: 'Identify posts with high emotional intensity and controversial topics'
  },
  {
    id: 'toxicity',
    label: 'Toxicity Filter',
    description: 'Filter out toxic, harmful, or inappropriate content'
  }
]

export default function ComingSoonPanel() {
  const [hoveredFeature, setHoveredFeature] = useState<string | null>(null)

  return (
    <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
      <h2 className="text-lg font-medium text-gray-900 mb-6">Advanced Filters</h2>
      
      <div className="space-y-4">
        <p className="text-sm text-gray-600 mb-4">
          The following filters are coming soon and will provide enhanced content analysis capabilities.
        </p>
        
        {comingSoonFeatures.map((feature) => (
          <div 
            key={feature.id}
            className="relative flex items-center justify-between p-3 bg-gray-50 rounded-lg border border-gray-200"
            onMouseEnter={() => setHoveredFeature(feature.id)}
            onMouseLeave={() => setHoveredFeature(null)}
          >
            <div className="flex items-center space-x-3">
              {/* Disabled toggle switch */}
              <div className="relative">
                <button
                  disabled
                  aria-label={`${feature.label} (coming soon)`}
                  className="relative inline-flex h-6 w-11 flex-shrink-0 cursor-not-allowed rounded-full border-2 border-gray-200 bg-gray-200 transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 opacity-50"
                  tabIndex={0}
                >
                  <span className="sr-only">{feature.label} (coming soon)</span>
                  <span className="pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out translate-x-0" />
                </button>
              </div>
              
              {/* Feature label */}
              <div>
                <span className="text-sm font-medium text-gray-500">
                  {feature.label}
                </span>
                <div className="flex items-center space-x-2">
                  <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                    Coming Soon
                  </span>
                </div>
              </div>
            </div>
            
            {/* Info icon */}
            <div className="text-gray-400">
              <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
              </svg>
            </div>
            
            {/* Tooltip */}
            {hoveredFeature === feature.id && (
              <div className="absolute left-0 top-full mt-2 z-10 w-72 p-3 bg-gray-900 text-white text-sm rounded-lg shadow-lg">
                <div className="relative">
                  {feature.description}
                  {/* Tooltip arrow */}
                  <div className="absolute bottom-full left-4 -mb-1 border-4 border-transparent border-b-gray-900"></div>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>
      
      <div className="mt-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
        <div className="flex items-start space-x-3">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
            </svg>
          </div>
          <div>
            <h3 className="text-sm font-medium text-blue-900">Machine Learning Analysis</h3>
            <p className="mt-1 text-sm text-blue-700">
              These advanced filters use machine learning models to analyze post content, sentiment, and context. 
              They will be available in a future update with improved accuracy and performance.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
} 